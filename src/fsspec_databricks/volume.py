import asyncio
import logging
import threading
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import UnsupportedOperation
from typing import Any, TypeVar

from aiohttp import (
    ClientError,
    ClientMiddlewareType,
    ClientSession,
    ClientTimeout,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import BadRequest, NotFound
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, VolumeInfo
from databricks.sdk.service.files import DirectoryEntry, GetMetadataResponse
from fsspec.asyn import sync
from fsspec_alt_strategies.profile import StorageProfile

from ._contrib.strategy.files.adaptive import (
    AdaptiveReadableFile,
    AdaptiveWritableFile,
)
from ._contrib.strategy.files.fsspec_async import AsyncFileAdapter
from ._contrib.strategy.files.fsspec_transaction import (
    AdaptiveWritableTransaction,
    TransactionFile,
)
from .base import BaseDatabricksFileSystem, root_dir_info
from .error import (
    error_mapping,
    file_exists_error,
    file_not_found_error,
    io_error,
    is_a_directory_error,
    not_a_directory_error,
    not_empty_error,
    permission_error,
)
from .http_client import (
    AioHttpSessionSupport,
    HttpFileMixin,
    HttpReadableMixin,
    check_response,
    response_error,
    workspace_authenticator,
)
from .path import dbfs_root, parse_volume_path
from .utils import to_datetime, to_rfc3339

T = TypeVar("T")

logger = logging.getLogger(__name__)

_MiB = 1024 * 1024

volumes_root_info = {
    "name": "/Volumes",
    "size": None,
    "type": "directory",
    "created": None,
    "modified": None,
    "islink": False,
}


def _map_info(
    info: CatalogInfo | SchemaInfo | VolumeInfo | DirectoryEntry | GetMetadataResponse,
    detail: bool,
    path: str | None = None,
) -> dict[str, Any] | str:
    """Map a catalog, schema, volume, directory entry or file metadata to an fsspec info dict, or to its name."""
    if isinstance(info, CatalogInfo | SchemaInfo | VolumeInfo):
        if isinstance(info, CatalogInfo):
            name = f"/Volumes/{info.name}"
        elif isinstance(info, SchemaInfo):
            name = f"/Volumes/{info.catalog_name}/{info.name}"
        else:
            name = f"/Volumes/{info.catalog_name}/{info.schema_name}/{info.name}"
        mapped = {
            "name": name,
            "size": None,
            "type": "directory",
            "created": to_datetime(info.created_at),
            "modified": to_datetime(info.updated_at),
            "islink": False,
        }
    elif isinstance(info, GetMetadataResponse):
        if path is None:
            raise ValueError("Path must be provided for GetMetadataResponse mapping.")
        mapped = {
            "name": path.rstrip("/"),
            "size": info.content_length,
            "type": "file",
            "created": None,
            "modified": to_datetime(info.last_modified),
            "islink": False,
            "content-type": info.content_type,
        }
    elif isinstance(info, DirectoryEntry):
        mapped = {
            "name": info.path.rstrip("/"),
            "size": info.file_size,
            "type": "directory" if info.is_directory else "file",
            "created": None,
            "modified": to_datetime(info.last_modified),
            "islink": False,
        }
    else:
        raise ValueError(f"Unsupported info type: {info} {type(info)}")

    return mapped if detail else mapped["name"]


def _parse(path: str) -> tuple[str | None, str | None, str | None, str | None, str]:
    """`parse_volume_path`, raising FileNotFoundError for paths outside /Volumes."""
    try:
        return parse_volume_path(path)
    except ValueError:
        raise file_not_found_error(path) from None


_IO_OFFLOAD = ("IO_OFFLOAD", None)
_FILES_API_V2 = ("FILES_API", "2.0")
_FILES_API_V3 = ("FILES_API", "3.0")


@dataclass(frozen=True)
class _Endpoint:
    """An endpoint of file transfers: its protocol, as the endpoints list names it, and its base URL.

    - `IO_OFFLOAD` moves data through presigned URLs, which the Files API 2.0 of its host gives.
    - `FILES_API` 2.0 and 3.0 take data themselves, with the workspace's credentials. 2.0 on another host than the
      workspace is the storage proxy.
    """

    kind: tuple[str, str | None]
    base_url: str
    multipart_upload_supported: bool

    @property
    def api_version(self) -> str:
        """The version of the Files API URLs: IO_OFFLOAD's are 2.0."""
        return "3.0" if self.kind == _FILES_API_V3 else "2.0"

    @property
    def presigned(self) -> bool:
        return self.kind == _IO_OFFLOAD


def _parse_endpoint(entry: Any, workspace_host: str) -> _Endpoint | None:
    """An entry of the endpoints list, or None if unusable: an unknown protocol, or an unsafe or malformed address.

    Follows the SDK's rules, but one: plain HTTP is allowed on the workspace host whenever the workspace uses it,
    for Files API 3.0 too.
    """
    if not isinstance(entry, dict):
        return None
    kind = (entry.get("api"), entry.get("api_version"))
    if kind not in (_IO_OFFLOAD, _FILES_API_V2, _FILES_API_V3):
        return None
    scheme, hostname = entry.get("scheme"), entry.get("hostname")
    if scheme not in ("http", "https") or not isinstance(hostname, str) or not hostname:
        return None
    base_url = f"{scheme}://{hostname}"
    try:
        parsed = urllib.parse.urlsplit(base_url)
        port = parsed.port
    except ValueError:
        return None
    # A bare host and port: no path, query or user, so requests go to the host named.
    if parsed.netloc != hostname or parsed.username is not None or port == 0:
        return None
    if scheme == "http":
        # Plain HTTP carries the credentials in the clear: only to a workspace reached that way, or the storage proxy.
        workspace = urllib.parse.urlsplit(workspace_host)
        if parsed.hostname == workspace.hostname:
            if workspace.scheme != "http":
                return None
        elif kind != _FILES_API_V2:
            return None
    # As the SDK reads it: an unset flag is false.
    return _Endpoint(kind, base_url, entry.get("multipart_upload_supported") is True)


class VolumeFileSystem(AioHttpSessionSupport, BaseDatabricksFileSystem):
    """Unity Catalog Volume file system.

    An async fsspec file system: its synchronous methods run on fsspec's event loop. Its recursive
    operations walk the folder tree with the tree strategies of fsspec-alt-strategies, copied into
    `_contrib.strategy`. Its files call the Files API with aiohttp, the rest the SDK.
    """

    storage_profile = StorageProfile(
        fs_type="tree",
        parents_required=False,
        implicit_directories=False,
        renames_files=False,
        renames_directories=False,
    )
    """What the Files API does.

    - It lists one directory per request, so the tree strategies suit it.
    - An upload creates the file's missing parent directories, as checked on a live workspace.
    - Directories are objects of their own: an empty one exists, and is deleted by a request of its own.
    - It has no server-side copy or rename, of files or directories.
    """

    # Measured on a live workspace: a find of a tree took 1.4 s at 32, 2.1 s at 16, and no less at 64.
    operation_concurrency: int = 32
    """The maximum number of concurrent API requests of a recursive operation, such as `rm` or `copy`."""

    # Measured on a live workspace: sequential reads are fastest at 16 to 24, and many small reads slow down
    # beyond 8, as the workspace throttles them.
    io_concurrency: int = 16
    """The maximum number of concurrent range reads and part uploads, shared by all the files opened."""

    _endpoints_timeout: float = 10.0
    """The timeout in seconds of the request for the endpoints list, which is never retried."""

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        connection_pool_size: int | None = None,
        **storage_options,
    ):
        """Initialize the Unity Catalog Volume file system.

        Parameters
        ----------
        client : WorkspaceClient, optional
            A Databricks SDK `WorkspaceClient` to call the APIs with.
        config : Config, optional
            A Databricks SDK `Config` to authenticate with.
        config_params : dict[str, Any], optional
            The parameters of a Databricks SDK `Config`, used without `client` or `config`.
        operation_concurrency : int, optional
            The maximum number of concurrent API requests of a recursive operation, such as `rm` or `copy`.
            Defaults to 16.
        io_concurrency : int, optional
            The maximum number of concurrent range reads and part uploads, shared by all the files opened.
            Defaults to 24.
        connection_pool_size : int, optional
            The maximum number of connections in the aiohttp connection pool. Defaults to 100.
        **storage_options
            Standard fsspec storage options, such as `asynchronous` and `loop`.
        """
        self._init_session(connection_pool_size)
        self._lock = threading.Lock()
        self._workspace_id: int | None = None
        self._endpoints: list[_Endpoint] | None = None

        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            **storage_options,
        )

    @property
    def workspace_id(self) -> int:
        """The ID of the workspace, fetched once."""
        with self._lock:
            if self._workspace_id is None:
                self._workspace_id = self.client.get_workspace_id()
            return self._workspace_id

    async def _get_endpoints(self) -> list["_Endpoint"]:
        """The endpoints of file transfers, best first: asked once, as the SDK asks, else the SDK's defaults."""
        if self._endpoints is None:
            host = self.config.host.rstrip("/")
            endpoints = [
                endpoint
                for entry in await self._fetch_endpoints(host)
                if (endpoint := _parse_endpoint(entry, host)) is not None
            ]
            self._endpoints = endpoints or [
                _Endpoint(_IO_OFFLOAD, host, multipart_upload_supported=True),
                _Endpoint(_FILES_API_V2, host, multipart_upload_supported=False),
            ]
        return self._endpoints

    async def _fetch_endpoints(self, host: str) -> list[Any]:
        """The entries of the endpoints list, or none if the workspace cannot answer: one request, not retried."""
        session = await self._get_session()
        try:
            async with session.get(
                f"{host}/api/2.0/fs/endpoints",
                middlewares=(workspace_authenticator(self.config),),
                timeout=ClientTimeout(total=self._endpoints_timeout),
            ) as response:
                logger.debug("Listing endpoints: status=%d", response.status)
                # A 404 before the API's rollout on the workspace.
                body = await response.json() if response.status == 200 else {}
        except (ClientError, TimeoutError, ValueError) as e:
            logger.debug("Listing endpoints: failed=%s", type(e).__name__)
            return []
        entries = body.get("endpoints") if isinstance(body, dict) else None
        return entries if isinstance(entries, list) else []

    ### File/directory information
    async def _info(self, path, include_browse: bool | None = None, **kwargs):
        if self._strip_protocol(path) == dbfs_root:
            return dict(root_dir_info)

        catalog, schema, volume, path_in_volume, full_path = _parse(path)

        with error_mapping(path):
            if catalog is None:
                return dict(volumes_root_info)
            elif schema is None:
                info = await self._run_in_thread(
                    self.client.catalogs.get, catalog, include_browse=include_browse
                )
            elif volume is None:
                info = await self._run_in_thread(
                    self.client.schemas.get,
                    f"{catalog}.{schema}",
                    include_browse=include_browse,
                )
            elif path_in_volume == "/":
                info = await self._run_in_thread(
                    self.client.volumes.read,
                    f"{catalog}.{schema}.{volume}",
                    include_browse=include_browse,
                )
            else:
                # Files first: they are asked about more often than directories
                try:
                    return await self._file_info(full_path)
                except NotFound:
                    return await self._directory_info(full_path)
            return _map_info(info, detail=True)

    async def _file_info(self, full_path: str) -> dict[str, Any]:
        """Info of a file in a volume, with one API call. Raises `NotFound` for other paths."""
        metadata = await self._run_in_thread(self.client.files.get_metadata, full_path)
        return _map_info(metadata, detail=True, path=full_path)

    async def _directory_info(self, full_path: str) -> dict[str, Any]:
        """Info of a directory in a volume, with one API call. Raises `NotFound` for other paths."""
        await self._run_in_thread(self.client.files.get_directory_metadata, full_path)
        return {
            "name": full_path.rstrip("/"),
            "size": None,
            "type": "directory",
            "created": None,
            "modified": None,
            "islink": False,
        }

    async def _isdir(self, path):
        try:
            _, _, _, path_in_volume, full_path = parse_volume_path(path)
        except ValueError:
            path_in_volume, full_path = None, None

        try:
            if path_in_volume is None or path_in_volume == "/":
                # The root, the volumes root, catalogs, schemas and volumes
                return (await self._info(path))["type"] == "directory"
            with error_mapping(path):
                await self._directory_info(full_path)
                return True
        except OSError:
            return False

    async def _isfile(self, path):
        try:
            _, _, _, path_in_volume, full_path = parse_volume_path(path)
        except ValueError:
            return False

        if path_in_volume is None or path_in_volume == "/":
            return False

        try:
            with error_mapping(path):
                await self._file_info(full_path)
                return True
        except Exception:
            return False

    async def _list(self, path, include_browse: bool | None = None, **kwargs):
        """List a folder a page at a time, as the SDK's listing fetches them: a catalog, a schema, a volume or a directory."""
        if self._strip_protocol(path) == dbfs_root:
            yield dict(volumes_root_info)
            return

        catalog, schema, volume, _, full_path = _parse(path)
        if catalog is None:
            entries = self._iter_in_thread(
                self.client.catalogs.list, include_browse=include_browse
            )
        elif schema is None:
            entries = self._iter_in_thread(
                self.client.schemas.list, catalog, include_browse=include_browse
            )
        elif volume is None:
            entries = self._iter_in_thread(
                self.client.volumes.list,
                catalog,
                schema,
                include_browse=include_browse,
            )
        else:
            entries = self._iter_in_thread(
                self.client.files.list_directory_contents, full_path
            )
        listed = False
        with error_mapping(path):
            try:
                async for entry in entries:
                    listed = True
                    yield _map_info(entry, detail=True)
            except (NotFound, BadRequest):
                if volume is None or listed:
                    raise
                # A file lists as itself
                yield await self._file_info(full_path)

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        try:
            src = await self.open_async(path1, "rb")
        except IsADirectoryError:
            await self._cp_directory(path2)
            return

        # The Files API has no copy: stream the content with concurrent reads and uploads
        async with src:
            dst = await self.open_async(path2, "wb")
            async with dst:
                while chunk := await src.read(self.transfer_size):
                    await dst.write(chunk)

    async def _rm_file(self, path, **kwargs):
        _, _, _, path_in_volume, full_path = _parse(path)
        if path_in_volume is None or path_in_volume == "/":
            raise is_a_directory_error(path)

        with error_mapping(path):
            try:
                await self._run_in_thread(self.client.files.delete, full_path)
            except (NotFound, BadRequest):
                if await self._isdir(path):
                    raise is_a_directory_error(path) from None
                raise

    async def _rmdir(self, path):
        _, _, _, path_in_volume, full_path = _parse(path)
        if path_in_volume is None or path_in_volume == "/":
            raise permission_error(
                path, "Catalogs, schemas and volumes are not deleted as directories"
            )

        # Checked first: deleting a missing directory succeeds on some clouds, e.g.
        # AWS, where rmdir must raise FileNotFoundError.
        info = await self._info(path)
        if info["type"] != "directory":
            raise not_a_directory_error(path)

        with error_mapping(path):
            try:
                await self._run_in_thread(self.client.files.delete_directory, full_path)
            except BadRequest as e:
                if "is not empty" in str(e):
                    raise not_empty_error(path) from e
                raise

    async def _create_directory(self, path):
        _, _, _, path_in_volume, full_path = _parse(path)
        if path_in_volume is None or path_in_volume == "/":
            raise permission_error(
                path, "Catalogs, schemas and volumes are not created as directories"
            )
        with error_mapping(path):
            await self._run_in_thread(self.client.files.create_directory, full_path)

    ### File I/O
    async def _check_open_target(self, path: str, mode: str) -> dict[str, Any] | None:
        """Check that the path can be opened in the mode, and return its info if needed."""
        if "r" not in mode and "x" not in mode:
            # Writing only needs to refuse directories
            if await self._isdir(path):
                raise is_a_directory_error(path)
            return None

        try:
            info = await self._info(path)
        except FileNotFoundError:
            info = None

        if info is not None and info["type"] == "directory":
            raise is_a_directory_error(path)
        if "r" in mode and info is None:
            raise file_not_found_error(path)
        if "x" in mode and info is not None:
            raise file_exists_error(path)

        return info

    async def _open_file(self, path: str, mode: str):
        """Open a file object on the running loop."""
        if "a" in mode or "+" in mode:
            raise UnsupportedOperation("Append and read/write modes are not supported.")

        info = await self._check_open_target(path, mode)
        _, _, _, path_in_volume, full_path = _parse(path)
        if path_in_volume is None or path_in_volume == "/":
            raise is_a_directory_error(path)

        endpoints = await self._get_endpoints()
        loop = asyncio.get_running_loop()
        session = await self._get_session()
        auth = workspace_authenticator(self.config)
        if "r" in mode:
            endpoint = endpoints[0]
            return VolumeReadableFile(
                full_path,
                loop,
                int(info["size"]),
                session=session,
                base_url=endpoint.base_url,
                auth=auth,
                api_version=endpoint.api_version,
                prefer_presigned_url=endpoint.presigned,
                concurrency_limit=self.io_limit,
            )
        else:
            # Parts need an endpoint taking multipart uploads. Else, as the SDK defaults: presigned URLs.
            endpoint = next(
                (e for e in endpoints if e.multipart_upload_supported),
                _Endpoint(_IO_OFFLOAD, self.config.host.rstrip("/"), True),
            )
            return VolumeWritableFile(
                full_path,
                loop,
                session=session,
                base_url=endpoint.base_url,
                auth=auth,
                api_version=endpoint.api_version,
                use_presigned_url=endpoint.presigned,
                concurrency_limit=self.io_limit,
            )

    async def open_async(self, path, mode="rb", **kwargs):
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError("Only binary mode without compression is supported")
        return AsyncFileAdapter(await self._open_file(path, mode))

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        f = sync(self.loop, self._open_file, path, mode)
        if autocommit or "r" in mode:
            return f
        # In a transaction, the upload is completed on commit, and aborted on discard.
        return TransactionFile(AdaptiveWritableTransaction(f))

    def open(self, path, mode="rb", **kwargs):
        """Open a file in a Unity Catalog Volume.

        Parameters
        ----------
        path : str
            The path to the file in the Unity Catalog Volume file system.
        mode : str, optional
            The file mode, such as `rb` or `w`. Append and update modes are not supported.
        **kwargs
            Passed to `fsspec.AbstractFileSystem.open`, e.g. `compression`, `encoding`, `errors` and
            `newline`. Options of the other backends, such as `notebook_format`, are ignored.
        """
        return super().open(path, mode, **kwargs)


class _VolumeFileMixin(HttpFileMixin):
    """The Files API URL of a file, shared by the file classes."""

    def _init_requests(
        self,
        path: str,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        api_version: str,
    ) -> None:
        self._init_http(session, base_url, auth)
        self.api_version = api_version
        self._files_url = (
            f"{self._base_url}/api/{api_version}/fs/files{urllib.parse.quote(path)}"
        )


class VolumeReadableFile(HttpReadableMixin, _VolumeFileMixin, AdaptiveReadableFile):
    """A Unity Catalog Volume file read by range requests, through presigned URLs or the Files API."""

    # Random reads of small records took 8.5 s at 64 KiB, against 11.5 s at 256 KiB.
    min_block_size: int = 64 * 1024
    """The smallest range request, which smaller reads fetch whole."""

    max_block_size: int = 8 * _MiB
    """The largest range request. Larger reads are split, to fetch the parts together."""

    # Many requests in flight beat fewer: a sequential read took 1.4 s at 32, against 1.9 s at 8 and 1.5 s at 64.
    spread: int = 32
    """How many requests a large read is split into. Sequential reads fetch up to `spread * max_block_size` ahead."""

    # Presigned URLs were faster even for 64 KB files.
    min_presigned_url_size: int = 0
    """The smallest file read through presigned URLs. Smaller files are read from the Files API directly."""

    presigned_url_expiration_duration: int = 60  # min
    """How many minutes the presigned URLs are valid."""

    presigned_url_refresh_after: int = 30  # min
    """After how many minutes a presigned URL is renewed."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        size: int,
        *,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        api_version: str = "2.0",
        prefer_presigned_url: bool = True,
        **kwargs,
    ):
        super().__init__(path, loop, size, **kwargs)
        self._init_requests(path, session, base_url, auth, api_version)

        self.use_presigned_url = (
            prefer_presigned_url and size >= self.min_presigned_url_size
        )

        self._presigned_url: str | None = None
        self._presigned_url_headers: dict[str, str] | None = None
        self._presigned_url_refresh_time: datetime | None = None
        self._presigned_url_lock = asyncio.Lock()

        # The version read: the first answer's Last-Modified, from the storage answering the ranges.
        self._last_modified: str | None = None

    async def _get_presigned_url(self) -> tuple[str, dict[str, str]]:
        """The presigned URL to read the file, with its headers: created anew when missing or due for renewal."""

        async with self._presigned_url_lock:
            if (
                self._presigned_url is None
                or datetime.now(tz=UTC) > self._presigned_url_refresh_time
            ):
                now = datetime.now(tz=UTC)
                expire_time = now + timedelta(
                    minutes=self.presigned_url_expiration_duration
                )
                refresh_time = now + timedelta(minutes=self.presigned_url_refresh_after)

                async with self._session.post(
                    f"{self._base_url}/api/2.0/fs/create-download-url",
                    params={"path": self.path, "expire_time": to_rfc3339(expire_time)},
                    middlewares=(self._auth,),
                    timeout=self._timeout,
                ) as response:
                    await check_response(response, self.path, "Creating presigned URL")
                    result = await response.json()

                self._presigned_url = result["url"]
                self._presigned_url_headers = {
                    h["name"]: h["value"] for h in result.get("headers", [])
                }
                self._presigned_url_refresh_time = refresh_time

        return self._presigned_url, dict(self._presigned_url_headers)

    async def _request_range(self, start: int, end: int) -> bytes:
        if self.use_presigned_url:
            url, headers = await self._get_presigned_url()
            middlewares = ()
        else:
            url = self._files_url
            headers = {}
            middlewares = (self._auth,)

        headers["Range"] = f"bytes={start}-{end - 1}"
        if self._last_modified is not None:
            # As the SDK reads: the server refuses the range of a file replaced since.
            headers["If-Unmodified-Since"] = self._last_modified

        async with self._session.get(
            url,
            headers=headers,
            middlewares=middlewares,
            timeout=self._timeout,
        ) as response:
            # Reads of ranges are not logged: a read makes many, and the lines would drown the rest.
            if response.status == 412:
                raise io_error(self.path, "The file changed while it was read")
            response.raise_for_status()
            # A server ignoring Range answers 200 with the whole file, which must not be taken for the range.
            if response.status != 206 and response.content_length != end - start:
                raise io_error(
                    self.path,
                    f"Range {start}-{end} answered with status {response.status}, "
                    f"length {response.content_length}",
                )
            self._check_version(response.headers.get("Last-Modified"))
            return await response.read()

    def _check_version(self, last_modified: str | None) -> None:
        """Keep the first answer's Last-Modified, and refuse another: concurrent first ranges go unconditionally."""
        if last_modified is None:
            return
        if self._last_modified is None:
            self._last_modified = last_modified
        elif last_modified != self._last_modified:
            raise io_error(self.path, "The file changed while it was read")


class VolumeWritableFile(_VolumeFileMixin, AdaptiveWritableFile):
    """A Unity Catalog Volume file uploaded whole, or in parts by a multipart upload, as the SDK uploads.

    The SDK speaks no other upload API since 0.163: an initiate-upload reply naming another, such as a resumable
    upload, raises an `OSError`.
    """

    min_block_size: int = 5 * _MiB
    """The smallest part but the last."""

    max_block_size: int = 16 * _MiB
    """The largest part."""

    min_multipart_size: int = 5 * _MiB
    """The size from which a file is uploaded in parts."""

    url_expiration_duration_min: int = 60  # 1 hour
    """How many minutes the upload and abort URLs are valid."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        api_version: str = "2.0",
        use_presigned_url: bool = True,
        **kwargs,
    ):
        super().__init__(path, loop, **kwargs)
        self._init_requests(path, session, base_url, auth, api_version)

        self.use_presigned_url = use_presigned_url
        # The multipart upload started: its token, and the query naming it in the Files API's requests.
        self._session_token: str | None = None
        self._upload: dict[str, str] | None = None

    def _url_expire_time(self) -> str:
        return to_rfc3339(
            datetime.now(tz=UTC) + timedelta(minutes=self.url_expiration_duration_min)
        )

    def _middlewares(self) -> tuple[ClientMiddlewareType, ...]:
        """Presigned URLs carry their own authorization."""
        return () if self.use_presigned_url else (self._auth,)

    async def _post_json(self, url: str, body: dict[str, Any], doing: str) -> Any:
        async with self._session.post(
            url, json=body, middlewares=(self._auth,), timeout=self._timeout
        ) as response:
            await check_response(response, self.path, doing)
            return await response.json()

    async def _upload_all(self, data: bytes) -> None:
        async def put() -> None:
            async with self._session.put(
                self._files_url,
                data=data,
                middlewares=(self._auth,),
                timeout=self._timeout,
            ) as response:
                await check_response(response, self.path, "Uploading")

        # Sent again whole: it overwrites, as no `overwrite` is sent.
        await self._retried(put, "Uploading")

    async def _start_multipart_upload(self) -> None:
        async def initiate() -> Any:
            async with self._session.post(
                self._files_url,
                params={"action": "initiate-upload"},
                middlewares=(self._auth,),
                timeout=self._timeout,
            ) as response:
                await check_response(response, self.path, "Initiating upload")
                return await response.json()

        # A session initiated twice leaves the first one unused.
        result = await self._retried(initiate, "Initiating upload")

        if "multipart_upload" in result:
            self._session_token = result["multipart_upload"]["session_token"]
            self._upload = {
                "upload_type": "multipart",
                "session_token": self._session_token,
            }
        elif "upload_id" in result:
            # Files API 3.0 names a multipart upload by its ID.
            self._session_token = result["upload_id"]
            self._upload = {"upload_id": self._session_token}
        else:
            raise io_error(self.path, f"Unexpected initiate-upload response: {result}")

    async def _upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> tuple[int, str]:
        assert self._upload is not None
        # The multipart upload API's part numbers are 1-based.
        part_number = index + 1
        doing = f"Uploading part {part_number}"

        async def put() -> tuple[int, str]:
            url, headers, params = await self._part_url(part_number)
            async with self._session.put(
                url,
                headers=headers,
                params=params,
                data=data,
                middlewares=self._middlewares(),
                timeout=self._timeout,
            ) as response:
                await check_response(response, self.path, doing)
                return part_number, response.headers.get("etag", "")

        # The server's errors as OSErrors of their kind, e.g. a PermissionError for a 403.
        try:
            # A part sent again replaces the first, under the same number.
            return await self._retried(put, doing)
        except (ClientError, TimeoutError) as e:
            raise response_error(self.path, e, doing) from e

    async def _part_url(
        self, part_number: int
    ) -> tuple[str, dict[str, str], dict[str, Any] | None]:
        """Where a part goes: a presigned URL, or the Files API with the upload in the query."""
        if self.use_presigned_url:
            result = await self._post_json(
                f"{self._base_url}/api/2.0/fs/create-upload-part-urls",
                {
                    "path": self.path,
                    "session_token": self._session_token,
                    "start_part_number": part_number,
                    "count": 1,
                    "expire_time": self._url_expire_time(),
                },
                "Getting URL for part upload",
            )
            url = result["upload_part_urls"][0]["url"]
            headers = {
                h["name"]: h["value"]
                for h in result["upload_part_urls"][0].get("headers", [])
            }
            headers["Content-Type"] = "application/octet-stream"
            return url, headers, None
        else:
            return (
                self._files_url,
                {"Content-Type": "application/octet-stream"},
                {**self._upload, "part_number": part_number},
            )

    async def _complete_multipart_upload(self, parts: list[Any]) -> None:
        assert self._upload is not None

        async def complete() -> None:
            async with self._session.post(
                self._files_url,
                params={"action": "complete-upload", **self._upload},
                json={
                    "parts": [
                        {"part_number": i, "etag": etag} for i, etag in sorted(parts)
                    ],
                },
                middlewares=(self._auth,),
                timeout=self._timeout,
            ) as response:
                await check_response(response, self.path, "Completing upload")

        await self._retried(complete, "Completing upload")

    async def _abort_multipart_upload(self) -> None:
        if self._upload is None:
            # No upload started, e.g. the initiation failed: nothing to abort.
            return
        logger.warning("Aborting upload: path=%s", self.path)
        # A POST to the Files API, with or without presigned URLs, as the SDK aborts. No abort URL is asked for:
        # Azure Databricks refuses them.
        try:
            async with self._session.post(
                self._files_url,
                params={"action": "abort-upload", **self._upload},
                middlewares=(self._auth,),
                timeout=self._timeout,
            ) as response:
                await check_response(response, self.path, "Aborting upload")
        except (OSError, ClientError) as e:
            # Best effort, as the SDK's: the caller gets the upload's own failure, or its discard succeeds.
            logger.warning("Failed to abort upload: path=%s, error=%s", self.path, e)


__all__ = [
    "VolumeFileSystem",
    "VolumeReadableFile",
    "VolumeWritableFile",
    "volumes_root_info",
]

import asyncio
import logging
import re
import sys
import threading
import urllib.parse
from datetime import UTC, datetime, timedelta
from io import UnsupportedOperation
from random import random
from typing import Any, TypeVar

from aiohttp import (
    ClientConnectionError,
    ClientError,
    ClientHandlerType,
    ClientMiddlewareType,
    ClientRequest,
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    TCPConnector,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.client_types import HostType
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
    access_error,
    error_mapping,
    file_exists_error,
    file_not_found_error,
    io_error,
    is_a_directory_error,
    not_a_directory_error,
    not_empty_error,
    permission_error,
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


class VolumeFileSystem(BaseDatabricksFileSystem):
    """Unity Catalog Volume file system.

    An async fsspec file system: its synchronous methods run on fsspec's event loop. Its recursive
    operations walk the folder tree with the tree strategies of fsspec-alt-strategies, copied into
    `_contrib.strategy`. Its files call the Files API with aiohttp, the rest the SDK.
    """

    storage_profile = StorageProfile(
        fs_type="tree",
        parents_required=False,
        implicit_folders=False,
        renames_files=False,
        renames_folders=False,
    )
    """What the Files API does.

    - It lists one directory per request, so the tree strategies suit it.
    - An upload creates the file's missing parent directories, as checked on a live workspace.
    - Directories are objects of their own: an empty one exists, and is deleted by a request of its own.
    - It has no server-side copy or rename, of files or directories.
    """

    io_concurrency: int = 24
    """The maximum number of concurrent range reads and part uploads, shared by all the files opened."""

    connection_pool_size: int = 100
    """The maximum number of connections in the aiohttp connection pool (TCPConnector limit)."""

    _storage_proxy_hostname: str = "http://storage-proxy.databricks.com"
    """The hostname of the Databricks storage proxy."""

    _storage_proxy_probe_timeout: float = 3.0
    """The timeout in seconds for the storage proxy probe request."""

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
        if connection_pool_size is not None:
            self.connection_pool_size = connection_pool_size

        self._lock = threading.Lock()
        self._workspace_id: int | None = None
        self._storage_proxy_available: bool | None = None

        # Made on the loop that uses it, as TCPConnector needs a running loop.
        self._session: ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None

        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            **storage_options,
        )

    def close(self):
        """Close the file system and its HTTP session."""
        if self.closed:
            return
        try:
            session, self._session = self._session, None
            loop, self._session_loop = self._session_loop, None
            if session is not None and not session.closed and not loop.is_closed():
                try:
                    running = asyncio.get_running_loop()
                except RuntimeError:
                    running = None
                if running is loop:
                    # Nothing can wait for it on its own loop.
                    loop.create_task(session.close())
                elif loop.is_running() and not sys.is_finalizing():
                    sync(loop, session.close, timeout=5.0)
        finally:
            super().close()

    async def _get_session(self) -> ClientSession:
        """The aiohttp session of the running loop, made on first use."""
        if self.closed:
            raise RuntimeError("The file system is already closed")
        loop = asyncio.get_running_loop()
        if (
            self._session is None
            or self._session.closed
            or self._session_loop is not loop
        ):
            self._session = ClientSession(
                connector=TCPConnector(limit=self.connection_pool_size)
            )
            self._session_loop = loop
        return self._session

    @property
    def workspace_id(self) -> int:
        """The ID of the workspace, fetched once."""
        with self._lock:
            if self._workspace_id is None:
                self._workspace_id = self.client.get_workspace_id()
            return self._workspace_id

    @property
    def storage_proxy_available(self) -> bool:
        """Whether the Databricks storage proxy is available for this workspace."""
        if self._storage_proxy_available is None:
            sync(self.loop, self._probe_storage_proxy)
        return self._storage_proxy_available

    async def _probe_storage_proxy(self) -> bool:
        if self._storage_proxy_available is not None:
            return self._storage_proxy_available

        workspace_id = await asyncio.to_thread(lambda: self.workspace_id)
        probe_url = (
            f"{self._storage_proxy_hostname}/api/2.0/fs/files"
            f"/DatabricksInternal/Probe/fullstack/wis?ew={workspace_id}"
        )
        session = await self._get_session()
        try:
            async with session.head(
                probe_url,
                middlewares=(_workspace_authenticator(self.config),),
                timeout=ClientTimeout(total=self._storage_proxy_probe_timeout),
            ) as response:
                available = response.status == 200
                outcome = response.status
        except (TimeoutError, ClientConnectionError) as e:
            # The proxy can be unresolvable or unreachable from this network.
            available = False
            outcome = type(e).__name__
        logger.debug(
            "HEAD storage proxy probe: workspace_id=%d, status=%s",
            workspace_id,
            outcome,
        )

        if not available:
            # Outside Databricks the proxy is normally absent: the fallback is expected.
            logger.debug(
                "Storage proxy unavailable, using the workspace host: workspace_id=%d",
                workspace_id,
            )
        self._storage_proxy_available = available
        return available

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

    async def _ils(self, path, include_browse: bool | None = None, **kwargs):
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

        if await self._probe_storage_proxy():
            base_url = self._storage_proxy_hostname
            use_presigned_url = False
        else:
            base_url = self.config.host
            use_presigned_url = True

        kwargs = {
            "session": await self._get_session(),
            "base_url": base_url,
            "auth": _workspace_authenticator(self.config),
            "concurrency_limit": self.io_limit,
        }
        loop = asyncio.get_running_loop()
        if "r" in mode:
            return VolumeReadableFile(
                full_path,
                loop,
                int(info["size"]),
                prefer_presigned_url=use_presigned_url,
                **kwargs,
            )
        else:
            return VolumeWritableFile(
                full_path, loop, use_presigned_url=use_presigned_url, **kwargs
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
        # In a transaction, the upload is completed on commit, and aborted on discard. The last part is held too:
        # a resumable upload completes with it, and the server chooses that API only once the upload starts.
        return TransactionFile(AdaptiveWritableTransaction(f, hold_last_part=True))

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


def _workspace_authenticator(config: Config) -> ClientMiddlewareType:
    cfg = config

    async def auth(
        request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        ### From https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/core.py
        headers = await asyncio.to_thread(cfg.authenticate)
        # Add X-Databricks-Org-Id header for workspace clients on unified hosts
        if cfg.workspace_id and cfg.host_type == HostType.UNIFIED:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id
        ### end

        request.headers.update(headers)
        return await handler(request)

    return auth


_retryable_statuses = frozenset({429, 500, 502, 503, 504})


def _response_error(path: str, error: Exception, doing: str) -> OSError:
    """An OSError for a failed request, by its HTTP status if it has one."""
    status = getattr(error, "status", None)
    message = f"{doing} failed" + (f" with status {status}" if status else "")
    if status == 404:
        return file_not_found_error(path, message)
    if status in (401, 403):
        return access_error(path, message)
    if status == 409:
        return file_exists_error(path, message)
    return io_error(path, message)


async def _check_response(response: ClientResponse, path: str, doing: str) -> None:
    """Log the request, and raise an OSError for a failed response, logging its body."""
    if response.ok:
        logger.debug("%s: path=%s, status=%d", doing, path, response.status)
        return
    try:
        body = await response.text()
    except ClientError:
        # The server may close the connection before a refused upload is sent.
        body = None
    logger.debug(
        "%s: path=%s, status=%d, response=%s", doing, path, response.status, body
    )
    try:
        response.raise_for_status()
    except ClientResponseError as e:
        raise _response_error(path, e, doing) from e


class _VolumeFileMixin:
    """Request settings shared by the file classes."""

    timeout_total: float | None = None
    """The maximum seconds for a whole request: connecting, sending and reading the response.
    `None` or `0` means no timeout.
    See the `aiohttp documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    timeout_connect: float | None = None
    """The maximum seconds to get a connection, including the wait for a free one from the pool.
    `None` or `0` means no timeout.
    See the `aiohttp documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    timeout_sock_connect: float | None = 30
    """The maximum seconds to connect to a peer for a new connection.
    `None` or `0` means no timeout.
    See the `aiohttp documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    timeout_sock_read: float | None = 120
    """The maximum seconds between two reads of data from a peer.
    `None` or `0` means no timeout.
    See the `aiohttp documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    def _init_requests(
        self,
        path: str,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._files_url = f"{self._base_url}/api/2.0/fs/files{urllib.parse.quote(path)}"
        self._auth = auth
        self._timeout = ClientTimeout(
            total=self.timeout_total,
            connect=self.timeout_connect,
            sock_connect=self.timeout_sock_connect,
            sock_read=self.timeout_sock_read,
        )


class VolumeReadableFile(_VolumeFileMixin, AdaptiveReadableFile):
    """A Unity Catalog Volume file read by range requests, through presigned URLs or the Files API."""

    min_block_size: int = 1 * _MiB
    """The smallest range request, which smaller reads fetch whole."""

    max_block_size: int = 4 * _MiB
    """The largest range request. Larger reads are split, to fetch the parts together."""

    min_presigned_url_size: int = 1024 * 1024
    """The smallest file read through presigned URLs. Smaller files are read from the Files API directly."""

    presigned_url_expiration_duration: int = 60  # min
    """How many minutes the presigned URLs are valid."""

    presigned_url_refresh_after: int = 30  # min
    """After how many minutes a presigned URL is renewed."""

    fetch_max_retries: int = 3
    """The maximum number of retries of a range request after a transient error."""

    fetch_retry_delay: float = 1.0
    """The base delay in seconds before the first retry of a range request. It doubles with each retry."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        size: int,
        *,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        prefer_presigned_url: bool = True,
        **kwargs,
    ):
        super().__init__(path, loop, size, **kwargs)
        self._init_requests(path, session, base_url, auth)

        self.use_presigned_url = (
            prefer_presigned_url and size >= self.min_presigned_url_size
        )

        self._presigned_url: str | None = None
        self._presigned_url_headers: dict[str, str] | None = None
        self._presigned_url_refresh_time: datetime | None = None
        self._presigned_url_lock = asyncio.Lock()

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
                    await _check_response(response, self.path, "Creating presigned URL")
                    result = await response.json()

                self._presigned_url = result["url"]
                self._presigned_url_headers = {
                    h["name"]: h["value"] for h in result.get("headers", [])
                }
                self._presigned_url_refresh_time = refresh_time

        return self._presigned_url, dict(self._presigned_url_headers)

    async def _fetch_range(self, start: int, end: int) -> bytes:
        try:
            delay = self.fetch_retry_delay
            for _ in range(self.fetch_max_retries):
                try:
                    return await self._request_range(start, end)
                except ClientResponseError as e:
                    if e.status not in _retryable_statuses:
                        raise
                    logger.warning(
                        "Retrying range request after status %d: path=%s, range=%d-%d",
                        e.status,
                        self.path,
                        start,
                        end,
                    )
                # The jitter keeps concurrent range requests from retrying together.
                await asyncio.sleep(delay * (1 + random()))
                delay *= 2
            return await self._request_range(start, end)
        except (ClientError, TimeoutError) as e:
            raise _response_error(
                self.path, e, f"Reading the range {start}-{end}"
            ) from e

    async def _request_range(self, start: int, end: int) -> bytes:
        if self.use_presigned_url:
            url, headers = await self._get_presigned_url()
            middlewares = ()
        else:
            url = self._files_url
            headers = {}
            middlewares = (self._auth,)

        headers["Range"] = f"bytes={start}-{end - 1}"

        async with self._session.get(
            url,
            headers=headers,
            middlewares=middlewares,
            timeout=self._timeout,
        ) as response:
            # Reads of ranges are not logged: a read makes many, and the lines would drown the rest.
            response.raise_for_status()
            return await response.read()


_range_pat = re.compile(r"bytes=(?P<start>\d+)-(?P<end>\d+)")


class VolumeWritableFile(_VolumeFileMixin, AdaptiveWritableFile):
    """A Unity Catalog Volume file uploaded whole, or in parts by the upload API the server chooses.

    The initiate-upload reply names the API of each upload: multipart or resumable. The server's
    choice is not fixed per cloud and may change with API versions, so the file follows it. The
    file keeps what both APIs share, and each API's requests live in its session class.
    """

    part_granularity: int = 256 * 1024
    """Every part but the last is a multiple of this size."""

    min_block_size: int = 5 * _MiB
    """The smallest part but the last. A multiple of `part_granularity`."""

    max_block_size: int = 16 * _MiB
    """The largest part. A multiple of `part_granularity`."""

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
        use_presigned_url: bool = True,
        **kwargs,
    ):
        # Checked before the file is set up, so that nothing is left to abort.
        for name in ("min_block_size", "max_block_size"):
            size = kwargs.get(name) or getattr(self, name)
            if size % self.part_granularity != 0:
                raise ValueError(
                    f"{name} is {size} but it must be a multiple of "
                    f"{self.part_granularity}: path={path}"
                )

        super().__init__(path, loop, **kwargs)
        self._init_requests(path, session, base_url, auth)

        self.use_presigned_url = use_presigned_url
        self._upload_session: _MultipartSession | _ResumableSession | None = None

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
            await _check_response(response, self.path, doing)
            return await response.json()

    async def _delete(
        self, url: str, headers: dict[str, str], params: dict[str, Any] | None
    ) -> None:
        async with self._session.delete(
            url,
            headers=headers,
            params=params,
            middlewares=self._middlewares(),
            data=b"",
            timeout=self._timeout,
        ) as response:
            await _check_response(response, self.path, "Aborting upload")

    async def _upload_all(self, data: bytes) -> None:
        async with self._session.put(
            self._files_url,
            data=data,
            middlewares=(self._auth,),
            timeout=self._timeout,
        ) as response:
            await _check_response(response, self.path, "Uploading")

    async def _start_multipart_upload(self) -> None:
        async with self._session.post(
            self._files_url,
            params={"action": "initiate-upload"},
            middlewares=(self._auth,),
            timeout=self._timeout,
        ) as response:
            await _check_response(response, self.path, "Initiating upload")
            result = await response.json()

        # The server chooses the API of each upload: the file follows it.
        if "multipart_upload" in result:
            self._upload_session = _MultipartSession(
                self, result["multipart_upload"]["session_token"]
            )
        elif "resumable_upload" in result:
            self._upload_session = await _ResumableSession.start(
                self, result["resumable_upload"]["session_token"]
            )
            # A resumable upload takes its parts one at a time, in order.
            self.sequential = True
        else:
            raise io_error(self.path, f"Unexpected initiate-upload response: {result}")

    async def _upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> tuple[int, str] | None:
        assert self._upload_session is not None
        # The server's errors as OSErrors of their kind, e.g. a PermissionError for a 403.
        try:
            return await self._upload_session.upload_part(data, start, end, index, last)
        except (ClientError, TimeoutError) as e:
            raise _response_error(self.path, e, f"Uploading part {index}") from e

    async def _complete_multipart_upload(self, parts: list[Any]) -> None:
        assert self._upload_session is not None
        await self._upload_session.complete(parts)

    async def _abort_multipart_upload(self) -> None:
        if self._upload_session is None:
            # No session started, e.g. the initiation failed: nothing to abort.
            return
        logger.warning("Aborting upload: path=%s", self.path)
        await self._upload_session.abort()


class _MultipartSession:
    """A multipart upload session: parts in parallel, completed with their ETags."""

    def __init__(self, file: VolumeWritableFile, session_token: str):
        self.file = file
        self.session_token = session_token

    async def upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> tuple[int, str]:
        file = self.file
        # The multipart upload API's part numbers are 1-based.
        part_number = index + 1

        url, headers, params = await self._part_url(part_number)

        async with file._session.put(
            url,
            headers=headers,
            params=params,
            data=data,
            middlewares=file._middlewares(),
            timeout=file._timeout,
        ) as response:
            await _check_response(response, file.path, f"Uploading part {part_number}")
            return part_number, response.headers.get("etag", "")

    async def _part_url(
        self, part_number: int
    ) -> tuple[str, dict[str, str], dict[str, Any] | None]:
        file = self.file
        if file.use_presigned_url:
            result = await file._post_json(
                f"{file._base_url}/api/2.0/fs/create-upload-part-urls",
                {
                    "path": file.path,
                    "session_token": self.session_token,
                    "start_part_number": part_number,
                    "count": 1,
                    "expire_time": file._url_expire_time(),
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
        return (
            file._files_url,
            {"Content-Type": "application/octet-stream"},
            {
                "session_token": self.session_token,
                "upload_type": "multipart",
                "part_number": part_number,
            },
        )

    async def complete(self, parts: list[Any]) -> None:
        file = self.file
        async with file._session.post(
            file._files_url,
            params={
                "action": "complete-upload",
                "upload_type": "multipart",
                "session_token": self.session_token,
            },
            json={
                "parts": [
                    {"part_number": i, "etag": etag} for i, etag in sorted(parts)
                ],
            },
            middlewares=(file._auth,),
            timeout=file._timeout,
        ) as response:
            await _check_response(response, file.path, "Completing upload")

    async def _abort_url(self) -> tuple[str, dict[str, str], dict[str, Any] | None]:
        file = self.file
        if file.use_presigned_url:
            result = await file._post_json(
                f"{file._base_url}/api/2.0/fs/create-abort-upload-url",
                {
                    "path": file.path,
                    "session_token": self.session_token,
                    "expire_time": file._url_expire_time(),
                },
                "Getting URL for upload abort",
            )
            url = result["abort_upload_url"]["url"]
            headers = {
                h["name"]: h["value"]
                for h in result["abort_upload_url"].get("headers", [])
            }
            headers["Content-Type"] = "application/octet-stream"
            return url, headers, None
        return (
            file._files_url,
            {"Content-Type": "application/json"},
            {"action": "abort-upload", "session_token": self.session_token},
        )

    async def abort(self) -> None:
        try:
            url, headers, params = await self._abort_url()
        except OSError as e:
            if (
                isinstance(e.__cause__, ClientResponseError)
                and e.__cause__.status == 400
            ):
                return  # Azure Databricks does not support abort of multipart upload
            raise
        await self.file._delete(url, headers, params)


class _ResumableSession:
    """A resumable upload session: parts one at a time, in order, the last completing it.

    Parts but the last are multiples of 256 KiB, as resumable uploads require.
    """

    def __init__(
        self,
        file: VolumeWritableFile,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any] | None,
    ):
        self.file = file
        self.url = url
        self.headers = headers
        self.params = params

    @classmethod
    async def start(
        cls, file: VolumeWritableFile, session_token: str
    ) -> "_ResumableSession":
        """Get where to send the parts, which is also where to abort the session."""
        if file.use_presigned_url:
            result = await file._post_json(
                f"{file._base_url}/api/2.0/fs/create-resumable-upload-url",
                {
                    "path": file.path,
                    "session_token": session_token,
                    "expire_time": file._url_expire_time(),
                },
                "Getting URL for resumable upload",
            )
            url = result["resumable_upload_url"]["url"]
            headers = {
                h["name"]: h["value"]
                for h in result["resumable_upload_url"].get("headers", [])
            }
            return cls(file, url, headers, None)
        return cls(
            file,
            file._files_url,
            {"Content-Type": "application/octet-stream"},
            {"session_token": session_token, "upload_type": "resumable"},
        )

    async def upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> None:
        file = self.file
        original_start = start
        while True:
            headers = dict(self.headers)
            headers["Content-Range"] = f"bytes {start}-{end - 1}/{end if last else '*'}"
            headers["Content-Type"] = "application/octet-stream"

            async with file._session.put(
                self.url,
                headers=headers,
                params=self.params,
                data=data[start - original_start :],
                timeout=file._timeout,
                middlewares=file._middlewares(),
            ) as response:
                logger.debug(
                    "PUT resumable part %d-%d: path=%s, status=%d",
                    start,
                    end,
                    file.path,
                    response.status,
                )
                if response.status in (200, 201) and last:
                    return  # Upload is done
                if response.status != 308:
                    raise io_error(
                        file.path,
                        f"Unexpected response for resumable upload: status={response.status}",
                    )
                m = _range_pat.match(response.headers.get("Range") or "")
                if m is None:
                    raise io_error(
                        file.path,
                        f"Invalid or missing Range header in resumable upload response: "
                        f"header={response.headers.get('Range')!r}",
                    )
                confirmed = int(m.group("end"))
                if confirmed == end - 1:
                    return
                # Send again what the server has not confirmed
                start = confirmed + 1

    async def complete(self, parts: list[Any]) -> None:
        pass  # The last part completes a resumable upload.

    async def abort(self) -> None:
        # The upload URL is known already: no request for another.
        await self.file._delete(self.url, self.headers, self.params)


__all__ = [
    "VolumeFileSystem",
    "VolumeReadableFile",
    "VolumeWritableFile",
    "volumes_root_info",
]

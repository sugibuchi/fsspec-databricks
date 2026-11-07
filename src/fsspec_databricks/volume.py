import asyncio
import contextlib
import logging
import re
import sys
import threading
import urllib.parse
from collections.abc import Callable
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
from fsspec_alt_strategies_common.profile import StorageProfile

from ._contrib.strategy.files.adaptive import (
    AdaptiveReadableFile,
    AdaptiveWritableFile,
)
from ._contrib.strategy.files.fsspec_async import AsyncFileAdapter
from ._contrib.strategy.tree.filesystem import TreeFileSystem
from .base import DatabricksClientSupport, _register_cleanup, root_dir_info
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
from .file import write_mode
from .path import dbfs_root, parse_volume_path, scheme, strip_scheme, unstrip_scheme
from .utils import to_datetime, to_rfc3339

T = TypeVar("T")

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
    """Map Unity Catalog CatalogInfo, SchemaInfo, VolumeInfo, DirectoryEntry or GetMetadataResponse
    to fsspec file info dict, or to the name only if not detail."""
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


class VolumeFileSystem(DatabricksClientSupport, TreeFileSystem):
    """Unity Catalog Volume file system.

    An async fsspec file system: its synchronous methods run on fsspec's event loop. Its recursive
    operations walk the folder tree with the tree strategies of fsspec-alt-strategies, copied into
    `_contrib.strategy`.
    """

    protocol = scheme
    root_marker = "/"

    storage_profile = StorageProfile()
    """Nothing is declared: the Files API has no server-side copy or rename."""

    operation_concurrency: int = 16
    """The maximum number of concurrent API requests of a recursive operation, such as listings and deletions."""

    io_concurrency: int = 24
    """The maximum number of concurrent range reads and part uploads, shared by all the files opened."""

    connection_pool_size: int = 100
    """The maximum number of connections in the aiohttp connection pool (TCPConnector limit)."""

    verbose_debug_log: bool = False
    """Whether to enable verbose debug logging for file system operations."""

    _storage_proxy_hostname: str = "http://storage-proxy.databricks.com"
    """The hostname of the Databricks storage proxy."""

    _storage_proxy_probe_timeout: float = 3.0
    """The timeout in seconds for the storage proxy probe request."""

    log = logging.getLogger(__name__)

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        connection_pool_size: int | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        """Initialize the Unity Catalog Volume file system.

        Parameters
        ----------
        client : WorkspaceClient, optional
            A Databricks SDK WorkspaceClient object.
        config : Config, optional
            A Databricks SDK Config object.
        config_params : dict[str, Any], optional
            A dictionary of parameters to create a Databricks SDK Config object.
        operation_concurrency : int, optional
            The maximum number of concurrent API requests of a recursive operation,
            such as `rm`, `copy`, `find` or `du`.
        io_concurrency : int, optional
            The maximum number of concurrent range reads and part uploads, shared by all the files
            opened by this file system.
        connection_pool_size : int, optional
            The maximum number of connections in the aiohttp connection pool.
        verbose_debug_log : bool, optional
            Whether to enable verbose debug logging for file system operations.
        **storage_options
            Standard fsspec storage options, such as `asynchronous` and `loop`.
        """
        super().__init__(
            operation_concurrency=operation_concurrency or self.operation_concurrency,
            io_concurrency=io_concurrency or self.io_concurrency,
            # Directory listing cache is explicitly disabled for all Databricks file systems.
            use_listings_cache=False,
            **storage_options,
        )
        self._init_client(client, config, config_params)

        if connection_pool_size is not None:
            self.connection_pool_size = connection_pool_size
        if verbose_debug_log is not None:
            self.verbose_debug_log = verbose_debug_log

        self._lock = threading.Lock()
        self._workspace_id: int | None = None
        self._storage_proxy_available: bool | None = None

        # Made on the loop that uses it, as TCPConnector needs a running loop.
        self._session: ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None

        _register_cleanup(self)
        self.log.debug("Opened: %s", self)

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)

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
            self._close_client()
            self.log.debug("Closed: %s", self)

    def __enter__(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        # Nothing can be done for a session left open when the interpreter exits.
        with contextlib.suppress(Exception):
            self.close()

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
            self.log.debug("aiohttp ClientSession created.")
        return self._session

    async def _sdk(self, func: Callable[[WorkspaceClient], T]) -> T:
        """Call the blocking Databricks SDK in a thread, so that the event loop goes on."""
        return await asyncio.to_thread(lambda: func(self.client))

    @property
    def workspace_id(self) -> int:
        """Get the workspace ID for this file system."""
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
        if self.verbose_debug_log:
            self.log.debug(
                "Check availability of the storage proxy: workspace_id=%d",
                workspace_id,
            )

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
        except (TimeoutError, ClientConnectionError):
            # The proxy can be unresolvable or unreachable from this network.
            available = False

        self.log.debug(
            "Storage proxy is available: workspace_id=%d"
            if available
            else "Storage proxy is not available: workspace_id=%d",
            workspace_id,
        )
        self._storage_proxy_available = available
        return available

    ### File/directory information
    async def _info(self, path, include_browse: bool | None = None, **kwargs):
        self.log.debug("Getting info: path=%s, include_browse=%s", path, include_browse)

        if self._strip_protocol(path) == dbfs_root:
            return dict(root_dir_info)

        catalog, schema, volume, path_in_volume, full_path = _parse(path)

        with error_mapping(path):
            if catalog is None:
                return dict(volumes_root_info)
            elif schema is None:
                info = await self._sdk(
                    lambda c: c.catalogs.get(catalog, include_browse=include_browse)
                )
            elif volume is None:
                info = await self._sdk(
                    lambda c: c.schemas.get(
                        f"{catalog}.{schema}", include_browse=include_browse
                    )
                )
            elif path_in_volume == "/":
                info = await self._sdk(
                    lambda c: c.volumes.read(
                        f"{catalog}.{schema}.{volume}", include_browse=include_browse
                    )
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
        metadata = await self._sdk(lambda c: c.files.get_metadata(full_path))
        return _map_info(metadata, detail=True, path=full_path)

    async def _directory_info(self, full_path: str) -> dict[str, Any]:
        """Info of a directory in a volume, with one API call. Raises `NotFound` for other paths."""
        await self._sdk(lambda c: c.files.get_directory_metadata(full_path))
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

    def created(self, path):
        return self.info(path)["created"]

    def modified(self, path):
        return self.info(path)["modified"]

    async def _ls(
        self, path, detail=True, include_browse: bool | None = None, **kwargs
    ):
        self.log.debug(
            "Listing path: path=%s, detail=%s, include_browse=%s",
            path,
            detail,
            include_browse,
        )

        if self._strip_protocol(path) == dbfs_root:
            return [dict(volumes_root_info)] if detail else [volumes_root_info["name"]]

        catalog, schema, volume, _, full_path = _parse(path)

        with error_mapping(path):
            if catalog is None:
                entries = await self._sdk(
                    lambda c: list(c.catalogs.list(include_browse=include_browse))
                )
            elif schema is None:
                entries = await self._sdk(
                    lambda c: list(
                        c.schemas.list(catalog, include_browse=include_browse)
                    )
                )
            elif volume is None:
                entries = await self._sdk(
                    lambda c: list(
                        c.volumes.list(catalog, schema, include_browse=include_browse)
                    )
                )
            else:
                try:
                    entries = await self._sdk(
                        lambda c: list(c.files.list_directory_contents(full_path))
                    )
                except (NotFound, BadRequest):
                    # A file lists as itself
                    info = await self._file_info(full_path)
                    return [info if detail else info["name"]]
        return [_map_info(e, detail=detail) for e in entries]

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        self.log.debug("Copying file: path1=%s, path2=%s", path1, path2)

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

    async def _cp_directory(self, path2):
        """Copy a directory: create the destination, as directories have no content."""
        try:
            info = await self._info(path2)
        except FileNotFoundError:
            await self._makedirs(path2, exist_ok=True)
            return
        if info["type"] != "directory":
            raise file_exists_error(path2)

    async def _rm_file(self, path, **kwargs):
        self.log.debug("Deleting file: path=%s", path)

        _, _, _, path_in_volume, full_path = _parse(path)
        if path_in_volume is None or path_in_volume == "/":
            raise is_a_directory_error(path)

        with error_mapping(path):
            try:
                await self._sdk(lambda c: c.files.delete(full_path))
            except (NotFound, BadRequest):
                if await self._isdir(path):
                    raise is_a_directory_error(path) from None
                raise

    async def _rmdir(self, path):
        self.log.debug("Deleting directory: path=%s", path)

        _, _, _, path_in_volume, full_path = _parse(path)
        if path_in_volume is None or path_in_volume == "/":
            raise permission_error(
                path, "Catalogs, schemas and volumes are not deleted as directories"
            )

        # Checked first, as the old precondition did: deleting a missing directory
        # succeeds on some clouds, e.g. AWS, where rmdir must raise FileNotFoundError.
        info = await self._info(path)
        if info["type"] != "directory":
            raise not_a_directory_error(path)

        with error_mapping(path):
            try:
                await self._sdk(lambda c: c.files.delete_directory(full_path))
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
            await self._sdk(lambda c: c.files.create_directory(full_path))

    async def _mkdir(self, path, create_parents=True, **kwargs):
        self.log.debug(
            "Making directory: path=%s, create_parents=%s", path, create_parents
        )

        parent = self._parent(path)
        if not await self._isdir(parent):
            # Tell a missing parent from a file
            try:
                await self._info(parent)
            except FileNotFoundError:
                if not create_parents:
                    raise
            else:
                raise not_a_directory_error(parent)

        if await self._exists(path):
            raise file_exists_error(path)

        # The Files API creates the parents too
        await self._create_directory(path)

    async def _makedirs(self, path, exist_ok=False):
        self.log.debug("Making directories: path=%s, exist_ok=%s", path, exist_ok)

        try:
            info = await self._info(path)
        except FileNotFoundError:
            await self._create_directory(path)
            return
        if info["type"] != "directory":
            raise not_a_directory_error(path)
        if not exist_ok:
            raise file_exists_error(path)

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
        self.log.debug("Opening file: path=%s, mode=%s", path, mode)

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
            "verbose_debug_log": self.verbose_debug_log,
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
        return sync(self.loop, self._open_file, path, mode)

    def open(self, path, mode="rb", **kwargs):
        """Open a file in a Unity Catalog Volume.

        Parameters
        ----------
        path : str
            The path to the file in the Unity Catalog Volume file system.
        mode : str, optional
            The file mode like `rb`, `w`. Append and read/write modes are not supported.
        **kwargs
            Passed to `fsspec.AbstractFileSystem.open`, e.g. `compression`, `encoding`, `errors` and
            `newline`. Other options of the other backends, such as `notebook_format`, are ignored.
        """
        if "t" in mode and write_mode(mode) and kwargs.get("newline") is None:
            kwargs["newline"] = "\n"
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
    """Raise an OSError for a failed response, logging its body."""
    if response.ok:
        return
    try:
        body = await response.text()
    except ClientError:
        # The server may close the connection before a refused upload is sent.
        body = None
    VolumeFileSystem.log.error("%s failed: path=%s, response=%s", doing, path, body)
    try:
        response.raise_for_status()
    except ClientResponseError as e:
        raise _response_error(path, e, doing) from e


class _VolumeFileMixin:
    """Request settings shared by the file classes."""

    timeout_total: float | None = None
    """The maximal number of seconds for the whole operation including connection establishment,
    request sending and response reading.
    If `None` or `0` is given, no timeout will be applied.
    See `aiohttp Documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    timeout_connect: float | None = None
    """The maximal number of seconds for connecting to a peer for a new connection,
    including wait for a free connection from a pool.
    If `None` or `0` is given, no timeout will be applied.
    See `aiohttp Documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    timeout_sock_connect: float | None = 30
    """The maximal number of seconds for connecting to a peer for a new connection.
    If `None` or `0` is given, no timeout will be applied.
    See `aiohttp Documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    timeout_sock_read: float | None = 120
    """The maximal number of seconds allowed for period between reading a new data portion from a peer.
    If `None` or `0` is given, no timeout will be applied.
    See `aiohttp Documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_."""

    log = VolumeFileSystem.log

    def _init_requests(
        self,
        path: str,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        verbose_debug_log: bool | None,
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
        self.verbose_debug_log = bool(verbose_debug_log)


class VolumeReadableFile(_VolumeFileMixin, AdaptiveReadableFile):
    """A Unity Catalog Volume file read by range requests, through presigned URLs or the Files API."""

    min_block_size: int = 1 * _MiB
    """The smallest range request, which smaller reads fetch whole."""

    max_block_size: int = 4 * _MiB
    """The largest range request. Larger reads are split, to fetch the parts together."""

    min_presigned_url_size: int = 1024 * 1024
    """The minimum file size to use presigned URLs for reading the file content. If the file size is smaller
    than this threshold, the file content will be read from the Files API without using presigned URLs."""

    presigned_url_expiration_duration: int = 60  # min
    """The duration in minutes for which the presigned URLs are valid."""

    presigned_url_refresh_after: int = 30  # min
    """The duration in minutes after which the presigned URL should be refreshed."""

    fetch_max_retries: int = 3
    """The maximal number of retries of a range request after a transient error."""

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
        verbose_debug_log: bool | None = None,
        **kwargs,
    ):
        super().__init__(path, loop, size, **kwargs)
        self._init_requests(path, session, base_url, auth, verbose_debug_log)

        self.use_presigned_url = (
            prefer_presigned_url and size >= self.min_presigned_url_size
        )

        self._presigned_url: str | None = None
        self._presigned_url_headers: dict[str, str] | None = None
        self._presigned_url_refresh_time: datetime | None = None
        self._presigned_url_lock = asyncio.Lock()

    async def _get_presigned_url(self) -> tuple[str, dict[str, str]]:
        """Get the presigned URL for reading the file content.

        If the current presigned URL is expired or not exists, a new presigned URL will be created."""

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
                self.log.debug("Created presigned URL: path=%s", self.path)

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
                    self.log.warning(
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
    """The duration in minutes for which the upload URLs and abort URLs are valid."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        use_presigned_url: bool = True,
        verbose_debug_log: bool | None = None,
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
        self._init_requests(path, session, base_url, auth, verbose_debug_log)

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
            self.log.debug("Upload mode - multipart: path=%s", self.path)
            self._upload_session = _MultipartSession(
                self, result["multipart_upload"]["session_token"]
            )
        elif "resumable_upload" in result:
            self.log.debug("Upload mode - resumable: path=%s", self.path)
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
        self.log.warning("Aborting upload: path=%s", self.path)
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
                if response.status in (200, 201) and last:
                    return  # Upload is done
                if response.status != 308:
                    file.log.error(
                        "Unexpected response for resumable upload: path=%s, status=%d",
                        file.path,
                        response.status,
                    )
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

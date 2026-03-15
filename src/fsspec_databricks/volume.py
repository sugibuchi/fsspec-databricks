import asyncio
import logging
import urllib.parse
from datetime import datetime, timedelta, timezone
from io import UnsupportedOperation
from threading import Thread
from time import sleep
from typing import Any

from aiohttp import (
    ClientHandlerType,
    ClientMiddlewareType,
    ClientRequest,
    ClientResponse,
    ClientTimeout,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.client_types import HostType
from databricks.sdk.config import Config
from databricks.sdk.errors import BadRequest, DatabricksError
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, VolumeInfo
from databricks.sdk.service.files import DirectoryEntry, GetMetadataResponse

from .base import AbstractDatabricksFileSystem, root_dir_info
from .dbfs import DBFS
from .error import (
    error_mapping,
    file_exists_error,
    file_not_found_error,
    is_a_directory_error,
    not_empty_error,
)
from .file import (
    AbstractAsyncReadableFile,
    AbstractAsyncWritableFile,
)
from .http import AioHttpClientMixin
from .path import dbfs_root, parse_volume_path, volumes_root
from .utils import to_datetime

volumes_root_info = {
    "name": volumes_root,
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
    """Map Unity Catalog CatalogInfo, SchemaInfo, VolumeInfo or GetMetadataResponse to fsspec file info dict."""
    if isinstance(info, CatalogInfo):
        if detail:
            return {
                "name": f"/Volumes/{info.name}",
                "size": None,
                "type": "directory",
                "created": to_datetime(info.created_at),
                "modified": to_datetime(info.updated_at),
                "islink": False,
            }
        else:
            return f"/Volumes/{info.name}"
    elif isinstance(info, SchemaInfo):
        if detail:
            return {
                "name": f"/Volumes/{info.catalog_name}/{info.name}",
                "size": None,
                "type": "directory",
                "created": to_datetime(info.created_at),
                "modified": to_datetime(info.updated_at),
                "islink": False,
            }
        else:
            return f"/Volumes/{info.catalog_name}/{info.name}"
    elif isinstance(info, VolumeInfo):
        if detail:
            return {
                "name": f"/Volumes/{info.catalog_name}/{info.schema_name}/{info.name}",
                "size": None,
                "type": "directory",
                "created": to_datetime(info.created_at),
                "modified": to_datetime(info.updated_at),
                "islink": False,
            }
        else:
            return f"/Volumes/{info.catalog_name}/{info.schema_name}/{info.name}"
    elif isinstance(info, GetMetadataResponse):
        if path is None:
            raise ValueError("Path must be provided for GetMetadataResponse mapping.")
        if detail:
            return {
                "name": path.rstrip("/"),
                "size": info.content_length,
                "type": "file",
                "created": None,
                "modified": to_datetime(info.last_modified),
                "islink": False,
                "content-type": info.content_type,
            }
        else:
            return path.rstrip("/")
    else:
        raise ValueError(f"Unsupported info type: {info} {type(info)}")


class VolumeFileSystem(DBFS):
    """Unity Catalog Volume file system.

    As `WorkspaceClient.dbfs` API is largely compatible with Unity Catalog Volumes, this class inherits
    `DBFS` and implements some handling specific for the volume-specific paths and file IO.
    """

    max_read_concurrency: int = 10
    """The maximum number of concurrent file read operations on a Unity Catalog Volume file."""

    min_read_block_size: int = 512 * 1024
    """The minimum data size to read for each read operation on a Unity Catalog Volume file."""

    max_read_block_size: int = 8 * 1024 * 1024
    """The maximum data size to read for each read operation on a Unity Catalog Volume file."""

    max_write_concurrency: int = 10
    """The maximum number of concurrent file write operations on a Unity Catalog Volume file."""

    min_write_block_size: int = 5 * 1024 * 1024
    """The minimum data size to write for each write operation on a Unity Catalog Volume file."""

    max_write_block_size: int = 32 * 1024 * 1024
    """The maximum data size to write for each write operation on a Unity Catalog Volume file."""

    min_multipart_upload_size: int = 5 * 1024 * 1024
    """The minimum file size to use multipart upload for uploading the file content."""

    log = logging.getLogger(__name__)

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        max_read_concurrency: int | None = None,
        min_read_block_size: int | None = None,
        max_read_block_size: int | None = None,
        max_write_concurrency: int | None = None,
        min_write_block_size: int | None = None,
        max_write_block_size: int | None = None,
        min_multipart_upload_size: int | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        """Initialize the Unity Catalog Volume file system."""
        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            verbose_debug_log=verbose_debug_log,
            **storage_options,
        )

        if max_read_concurrency:
            self.max_read_concurrency = max_read_concurrency
        if min_read_block_size:
            self.min_read_block_size = min_read_block_size
        if max_read_block_size:
            self.max_read_block_size = max_read_block_size

        if max_write_concurrency:
            self.max_write_concurrency = max_write_concurrency
        if min_write_block_size:
            self.min_write_block_size = min_write_block_size
        if max_write_block_size:
            self.max_write_block_size = max_write_block_size
        if min_multipart_upload_size:
            self.min_multipart_upload_size = min_multipart_upload_size

        self.__loop = None
        self.__io_thread: Thread | None = None

    def __getstate__(self):
        state = super().__getstate__()
        del state["_VolumeFileSystem__loop"]
        del state["_VolumeFileSystem__io_thread"]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self.__loop = None
        self.__io_thread = None

    @property
    def _loop(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__loop is None:

            def run_loop():
                self.__loop = asyncio.new_event_loop()
                self.log.debug("Running event loop in IO thread.")
                asyncio.set_event_loop(self.__loop)

                try:
                    self.__loop.run_forever()
                finally:
                    self.log.debug("Shutting down async tasks.")
                    self.__loop.run_until_complete(self.__loop.shutdown_asyncgens())
                    self.log.debug("Closing event loop.")
                    self.__loop.close()
                    self.log.debug("Event loop closed. Finishing IO thread.")

            self.__io_thread = Thread(
                target=run_loop, name="dbfs-io-thread", daemon=True
            )

            self.__io_thread.start()

            for _ in range(50):
                if self.__loop is not None and self.__loop.is_running():
                    break
                sleep(0.1)

            if self.__loop is None or not self.__loop.is_running():
                raise RuntimeError("Event loop in IO thread failed to start")

        return self.__loop

    def close(self):
        if not self.closed:
            try:
                if self.__loop is not None:
                    self.log.debug("Stopping event loop.")
                    self.__loop.call_soon_threadsafe(self.__loop.stop)
                    self.__io_thread.join(timeout=5.0)
                    if self.__io_thread.is_alive():
                        raise RuntimeError("IO thread failed to stop")
                    self.log.debug("IO thread stopped.")
            finally:
                self.__loop = None
                self.__io_thread = None
                super().close()

    @staticmethod
    def _ensure_in_volume(*paths: str):
        """Ensure the given paths are within Unity Catalog Volumes"""
        for path in paths:
            _, _, _, path_in_volume, _ = parse_volume_path(path)
            if path_in_volume is None:
                raise ValueError(f"Path {path} is not within a Unity Catalog Volume")

    ### File/directory information
    def info(self, path, include_browse: bool | None = None, **kwargs):
        self.log.debug("Getting info: path=%s, include_browse=%s", path, include_browse)

        stripped = self._strip_protocol(path)
        if stripped == dbfs_root:
            return root_dir_info

        try:
            catalog, schema, volume, path_in_volume, full_path = parse_volume_path(path)
        except ValueError:
            raise file_not_found_error(path) from None

        with error_mapping(path):
            if catalog is None:
                return volumes_root_info
            elif schema is None:
                return _map_info(
                    self.client.catalogs.get(catalog, include_browse=include_browse),
                    detail=True,
                )
            elif volume is None:
                return _map_info(
                    self.client.schemas.get(
                        f"{catalog}.{schema}", include_browse=include_browse
                    ),
                    detail=True,
                )
            elif path_in_volume == "/":
                return _map_info(
                    self.client.volumes.read(
                        f"{catalog}.{schema}.{volume}", include_browse=include_browse
                    ),
                    detail=True,
                )
            else:
                try:
                    self.client.files.get_directory_metadata(full_path)
                    return {
                        "name": full_path.rstrip("/"),
                        "size": None,
                        "type": "directory",
                        "created": None,
                        "modified": None,
                        "islink": False,
                    }
                except DatabricksError:
                    metadata = self.client.files.get_metadata(full_path)
                    return _map_info(metadata, detail=True, path=full_path)

    def ls(self, path, detail=True, include_browse: bool | None = None, **kwargs):
        self.log.debug(
            "Listing path: path=%s, detail=%s, include_browse=%s",
            path,
            detail,
            include_browse,
        )

        stripped = self._strip_protocol(path)
        if stripped == dbfs_root:
            return [volumes_root_info] if detail else [volumes_root]

        try:
            catalog, schema, volume, _, _ = parse_volume_path(path)
        except ValueError:
            raise file_not_found_error(path) from None

        with error_mapping(path):
            if catalog is None:
                return [
                    _map_info(c, detail=detail)
                    for c in self.client.catalogs.list(include_browse=include_browse)
                ]
            elif schema is None:
                return [
                    _map_info(s, detail=detail)
                    for s in self.client.schemas.list(
                        catalog, include_browse=include_browse
                    )
                ]
            elif volume is None:
                return [
                    _map_info(v, detail=detail)
                    for v in self.client.volumes.list(
                        catalog, schema, include_browse=include_browse
                    )
                ]
            else:
                return super().ls(path, detail=detail)

    def rmdir(self, path):
        # The precondition check is implemented in the base class.
        AbstractDatabricksFileSystem.rmdir(self, path=path)

        _, _, _, _, full_path = parse_volume_path(path)

        with error_mapping(path):
            try:
                self.client.files.delete_directory(full_path)
            except BadRequest as e:
                if "is not empty" in e.args[0]:
                    raise not_empty_error(path) from e
                else:
                    raise e

    def _open(
        self,
        path,
        mode="rb",
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        block_size: int | None = None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        if "a" in mode or "+" in mode:
            raise UnsupportedOperation("Append and read/write modes are not supported.")

        try:
            info = self.info(path)
            if info["type"] == "directory":
                raise is_a_directory_error(path)
        except FileNotFoundError:
            info = None

        if "r" in mode and info is None:
            raise file_not_found_error(path)

        if "x" in mode and info is not None:
            raise file_exists_error(path)

        if "r" in mode:
            return VolumeReadableFile(
                path=path,
                workspace_config=self.config,
                loop=self._loop,
                size=int(info.get("size")),
                max_concurrency=max_concurrency or self.max_read_concurrency,
                min_block_size=min_block_size or self.min_read_block_size,
                max_block_size=max_block_size or self.max_read_block_size,
                block_size=block_size,
                verbose_debug_log=self.verbose_debug_log,
            )
        else:
            return VolumeWritableFile(
                path=path,
                workspace_config=self.config,
                loop=self._loop,
                max_concurrency=max_concurrency or self.max_write_concurrency,
                min_block_size=min_block_size or self.min_write_block_size,
                max_block_size=max_block_size or self.max_write_block_size,
                min_multipart_upload_size=self.min_multipart_upload_size,
                block_size=block_size,
                verbose_debug_log=self.verbose_debug_log,
            )

    def open(
        self,
        path,
        mode="rb",
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        block_size: int | None = None,
        compression: str | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        **kwargs,
    ):
        return super().open(
            path=path,
            mode=mode,
            max_concurrency=max_concurrency,
            min_block_size=min_block_size,
            max_block_size=max_block_size,
            block_size=block_size,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )


def _workspace_authenticator(config: Config) -> ClientMiddlewareType:
    cfg = config

    async def auth(
        request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        ### From https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/core.py
        headers = cfg.authenticate()
        # Add X-Databricks-Org-Id header for workspace clients on unified hosts
        if cfg.workspace_id and cfg.host_type == HostType.UNIFIED:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id
        ### end

        request.headers.update(headers)
        return await handler(request)

    return auth


class VolumeReadableFile(AbstractAsyncReadableFile, AioHttpClientMixin):
    log = VolumeFileSystem.log

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

    def __init__(
        self,
        path: str,
        workspace_config: Config,
        loop,
        size: int,
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            loop=loop,
            size=size,
            max_concurrency=max_concurrency,
            min_block_size=min_block_size,
            max_block_size=max_block_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )

        self._config_session(
            base_url=workspace_config.host,
            middlewares=(_workspace_authenticator(workspace_config),),
            timeout=ClientTimeout(
                total=self.timeout_total,
                connect=self.timeout_connect,
                sock_connect=self.timeout_sock_connect,
                sock_read=self.timeout_sock_read,
            ),
        )

        _, _, _, _, self._posix_path = parse_volume_path(path)

    async def _fetch_range(self, start: int, end: int) -> bytes:
        async with self._session.get(
            f"/api/2.0/fs/files{urllib.parse.quote(self._posix_path)}",
            headers={"Range": f"bytes={start}-{end - 1}"},
        ) as response:
            response.raise_for_status()
            return await response.read()

    async def aclose(self):
        if not self.closed:
            try:
                await super().aclose()
            finally:
                await self._close_session()


class VolumeWritableFile(AbstractAsyncWritableFile, AioHttpClientMixin):
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

    url_expiration_duration_min: int = 60  # 1 hour
    """The duration in minutes for which the upload URLs and abort URLs are valid."""

    log = VolumeFileSystem.log

    def __init__(
        self,
        path: str,
        workspace_config: Config,
        loop,
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        min_multipart_upload_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            loop=loop,
            max_concurrency=max_concurrency,
            min_block_size=min_block_size,
            max_block_size=max_block_size,
            min_multipart_upload_size=min_multipart_upload_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )
        self._auth = _workspace_authenticator(workspace_config)
        self._upload_part_timeout = ClientTimeout(
            total=self.timeout_total,
            connect=self.timeout_connect,
            sock_connect=self.timeout_sock_connect,
            sock_read=self.timeout_sock_read,
        )

        self._config_session(base_url=workspace_config.host)

        _, _, _, _, self._posix_path = parse_volume_path(path)

        self.url_expiration_duration = timedelta(
            minutes=self.url_expiration_duration_min
        )

        self._session_token: str | None = None

    def _url_expire_time(self) -> str:
        expiration_time = datetime.now(tz=timezone.utc) + self.url_expiration_duration
        return expiration_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    async def _upload_all(self, data: bytes) -> None:
        async with self._session.put(
            f"/api/2.0/fs/files{urllib.parse.quote(self._posix_path)}",
            data=data,
            middlewares=(self._auth,),
        ) as response:
            response.raise_for_status()

    async def _start_multipart_upload(self) -> None:
        async with self._session.post(
            f"/api/2.0/fs/files{urllib.parse.quote(self._posix_path)}",
            params={"action": "initiate-upload"},
            middlewares=(self._auth,),
        ) as response:
            result = await response.json()
            try:
                response.raise_for_status()
            except:
                self.log.error(
                    "Failed to initiate upload: path=%s, response=%s", self.path, result
                )
                raise

            self._session_token = result["multipart_upload"]["session_token"]

    async def _upload_part(
        self, data: bytes, start: int, end: int, part_index: int
    ) -> tuple[int, str]:
        async with self._session.post(
            "/api/2.0/fs/create-upload-part-urls",
            json={
                "path": self._posix_path,
                "session_token": self._session_token,
                "start_part_number": part_index + 1,
                "count": 1,
                "expire_time": self._url_expire_time(),
            },
            middlewares=(self._auth,),
        ) as response:
            result = await response.json()
            try:
                response.raise_for_status()
            except:
                self.log.error(
                    "Failed to get URL for part upload: path=%s, response=%s",
                    self.path,
                    result,
                )
                raise

        upload_url = result["upload_part_urls"][0]["url"]
        headers = {"Content-Type": "application/octet-stream"}
        headers.update(
            {
                h["name"]: h["value"]
                for h in result["upload_part_urls"][0].get("headers", [])
            }
        )

        async with self._session.put(
            upload_url,
            headers=headers,
            data=data,
            timeout=self._upload_part_timeout,
        ) as response:
            response.raise_for_status()
            return part_index + 1, response.headers.get("etag", "")

    async def _complete_multipart_upload(self) -> None:
        parts = [await t for t in self._tasks]
        parts.sort()

        async with self._session.post(
            f"/api/2.0/fs/files{urllib.parse.quote(self._posix_path)}",
            params={
                "action": "complete-upload",
                "upload_type": "multipart",
                "session_token": self._session_token,
            },
            json={
                "parts": [{"part_number": i, "etag": etag} for i, etag in parts],
            },
            middlewares=(self._auth,),
        ) as response:
            result = await response.json()
            try:
                response.raise_for_status()
            except:
                self.log.error(
                    "Failed to complete upload: path=%s, response=%s", self.path, result
                )
                raise

    async def _abort_multipart_upload(self) -> None:
        async with self._session.post(
            "/api/2.0/fs/create-abort-upload-url",
            json={
                "path": self._posix_path,
                "session_token": self._session_token,
                "expire_time": self._url_expire_time(),
            },
            middlewares=(self._auth,),
        ) as response:
            result = await response.json()
            try:
                response.raise_for_status()
            except BadRequest:
                self.log.warning(
                    "Failed to get URL for upload abort: path=%s, response=%s",
                    self.path,
                    result,
                )
                return

        abort_url = result["abort_upload_url"]["url"]

        headers = {"Content-Type": "application/octet-stream"}
        headers.update(
            {
                h["name"]: h["value"]
                for h in result["abort_upload_url"].get("headers", [])
            }
        )

        async with self._session.delete(
            abort_url, headers=headers, data=b""
        ) as response:
            response.raise_for_status()

    async def aclose(self):
        if not self.closed:
            try:
                await super().aclose()
            finally:
                await self._close_session()


__all__ = [
    "VolumeFileSystem",
    "volumes_root_info",
]

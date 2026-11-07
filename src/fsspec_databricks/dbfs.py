import logging
from io import UnsupportedOperation
from typing import Any

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.files import DbfsAPI, FileInfo
from fsspec.asyn import sync
from fsspec_alt_strategies_common.profile import StorageProfile

from ._contrib.strategy.files.fsspec_async import SyncFileAdapter
from .base import DatabricksTreeFileSystem
from .error import (
    error_mapping,
    is_a_directory_error,
    not_a_directory_error,
    not_empty_error,
)
from .path import parse_dbfs_path
from .utils import to_datetime


def map_info(info: FileInfo, detail: bool) -> dict[str, Any] | str:
    if detail:
        return {
            "name": info.path.rstrip("/") or "/",
            "size": info.file_size,
            "type": "directory" if info.is_dir else "file",
            "created": None,
            "modified": to_datetime(info.modification_time),
            "islink": False,
        }
    else:
        return info.path.rstrip("/") or "/"


class DBFS(DatabricksTreeFileSystem):
    """Databricks DBFS API.

    An async fsspec file system: its synchronous methods run on fsspec's event loop, and the SDK's calls in
    threads. Its recursive operations are the tree strategies of fsspec-alt-strategies. A recursive `rm`
    deletes each directory by the API's own recursive delete, `_rm_tree`.
    """

    storage_profile = StorageProfile(renames_folders=True)
    """The API's move moves a directory with all in it, in one request."""

    log = logging.getLogger(__name__)

    def __init__(
        self,
        client=None,
        config=None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        """Initialize the DBFS file system.

        Parameters
        ----------
        client : WorkspaceClient, optional
            A Databricks SDK WorkspaceClient object.
        config : Config, optional
            A Databricks SDK Config object.
        config_params : dict[str, Any], optional
            A dictionary of parameters to create a Databricks SDK Config object.
        operation_concurrency : int, optional
            The maximum number of concurrent API requests of a recursive operation, such as `copy` or `find`.
        io_concurrency : int, optional
            Unused: DBFS files are the SDK's own, which read and write one block at a time.
        verbose_debug_log : bool, optional
            Whether to enable verbose debug logging for file system operations.
        **storage_options
            Standard fsspec storage options, such as `asynchronous` and `loop`.
        """
        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            verbose_debug_log=verbose_debug_log,
            **storage_options,
        )

    ### File/directory information
    async def _info(self, path, **kwargs):
        self.log.debug("Getting info: path=%s", path)

        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            status = await self._sdk(lambda c: c.dbfs.get_status(dbfs_path))
        return map_info(status, detail=True)

    async def _ls(self, path, detail=True, **kwargs):
        self.log.debug("Listing directory: path=%s, detail=%s", path, detail)

        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            entries = await self._sdk(lambda c: list(c.dbfs.list(dbfs_path)))
        return [map_info(i, detail) for i in entries]

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        self.log.debug("Copying file: path1=%s, path2=%s", path1, path2)

        if (await self._info(path1))["type"] == "directory":
            await self._cp_directory(path2)
            return

        src, dst = parse_dbfs_path(path1), parse_dbfs_path(path2)
        with error_mapping(path1):
            await self._sdk(
                lambda c: c.dbfs.copy(src, dst, recursive=False, overwrite=True)
            )

    async def _mv_file(self, path1, path2, **kwargs):
        """Move a file, or a directory with all in it, with the API's move: one request."""
        self.log.debug("Moving: path1=%s, path2=%s", path1, path2)

        src, dst = parse_dbfs_path(path1), parse_dbfs_path(path2)
        with error_mapping(path1):
            await self._sdk(lambda c: c.dbfs.move(src, dst))

    async def _delete(self, path: str, recursive: bool) -> None:
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            # The API's own delete: the SDK's refuses directories unless recursive.
            await self._sdk(
                lambda c: DbfsAPI.delete(c.dbfs, dbfs_path, recursive=recursive)
            )

    async def _rm_file(self, path, **kwargs):
        self.log.debug("Deleting file: path=%s", path)

        if (await self._info(path))["type"] != "file":
            raise is_a_directory_error(path)
        await self._delete(path, recursive=False)

    async def _rm_tree(self, path):
        """Delete a directory with all in it, by the API's recursive delete: one request."""
        self.log.debug("Deleting directory tree: path=%s", path)
        await self._delete(path, recursive=True)

    async def _rmdir(self, path):
        self.log.debug("Deleting directory: path=%s", path)

        if (await self._info(path))["type"] != "directory":
            raise not_a_directory_error(path)
        if await self._ls(path, detail=False):
            raise not_empty_error(path)

        try:
            # Not recursive: the API refuses a directory filled meanwhile.
            await self._delete(path, recursive=False)
        except DatabricksError as e:
            if getattr(e, "error_code", None) == "IO_ERROR":
                raise not_empty_error(path) from e
            raise

    async def _create_directory(self, path):
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            await self._sdk(lambda c: c.dbfs.mkdirs(dbfs_path))

    ### File I/O
    async def _open_handle(self, path: str, mode: str) -> tuple[Any, int | None]:
        """Open the SDK's file object in a thread, and tell the size of a file read."""
        self.log.debug("Opening file: path=%s, mode=%s", path, mode)

        if "a" in mode or "+" in mode:
            raise UnsupportedOperation("Append and read/write modes are not supported.")

        size = None
        if "r" in mode:
            info = await self._info(path)
            if info["type"] == "directory":
                raise is_a_directory_error(path)
            size = info["size"]
        elif await self._isdir(path):
            raise is_a_directory_error(path)

        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            handle = await self._sdk(
                lambda c: c.dbfs.open(
                    dbfs_path,
                    read="r" in mode,
                    write="r" not in mode,
                    overwrite="x" not in mode,
                )
            )
        return handle, size

    async def open_async(self, path, mode="rb", **kwargs):
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError("Only binary mode without compression is supported")
        handle, size = await self._open_handle(path, mode)
        return _DbfsFileAdapter(handle, mode, size)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        handle, _ = sync(self.loop, self._open_handle, path, mode)
        return handle

    def open(self, path, mode="rb", **kwargs):
        """Open a file in the legacy DBFS.

        Parameters
        ----------
        path : str
            The path to the file in the legacy DBFS.
        mode : str, optional
            The file mode like `rb`, `w`. Append and read/write modes are not supported.
        **kwargs
            Passed to `fsspec.AbstractFileSystem.open`. Options of the other backends, such as
            `notebook_format`, are ignored.
        """
        return super().open(path, mode, **kwargs)


class _DbfsFileAdapter(SyncFileAdapter):
    """The SDK's DBFS file object for `open_async`, with the mode and size it does not tell itself."""

    def __init__(self, file, mode: str, size: int | None):
        super().__init__(file)
        self._mode = mode
        self._size = size

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def size(self) -> int | None:
        return self._size


__all__ = [
    "DBFS",
]

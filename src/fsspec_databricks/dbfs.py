import asyncio
import base64
import contextlib
import functools
import logging
from io import UnsupportedOperation
from typing import Any

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.files import DbfsAPI, FileInfo
from fsspec.asyn import sync
from fsspec_alt_strategies_common.profile import StorageProfile

from ._contrib.strategy.files.adaptive import (
    AdaptiveReadableFile,
    AdaptiveWritableFile,
)
from ._contrib.strategy.files.fsspec_async import AsyncFileAdapter
from ._contrib.strategy.files.fsspec_transaction import (
    TempPathTransaction,
    TransactionFile,
)
from .base import DatabricksTreeFileSystem
from .error import (
    error_mapping,
    file_exists_error,
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


logger = logging.getLogger(__name__)

_MiB = 1024 * 1024

_max_request_size = _MiB
"""The most bytes a read, an add-block or a put with contents takes, before base64 encoding.

The reference sets 1 MB for each. The SDK's own files send 1 MiB of raw bytes per add-block, so the limit is on the
decoded data.
"""


class DBFS(DatabricksTreeFileSystem):
    """Legacy DBFS file system, on the DBFS API.

    An async fsspec file system: its synchronous methods run on fsspec's event loop, and the SDK's calls in
    threads. Its recursive operations are the tree strategies of fsspec-alt-strategies. A recursive `rm`
    deletes each directory by the API's own recursive delete, `_rm_tree`.
    """

    storage_profile = StorageProfile(renames_folders=True)
    """The API's move moves a directory with all in it, in one request."""

    def __init__(
        self,
        client=None,
        config=None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        **storage_options,
    ):
        """Initialize the legacy DBFS file system.

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
            The maximum number of concurrent reads and writes of files, which also adds to the threads running SDK
            calls. Defaults to 8.
        **storage_options
            Standard fsspec storage options, such as `asynchronous` and `loop`.
        """
        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            **storage_options,
        )

    ### File/directory information
    async def _info(self, path, **kwargs):
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            status = await self._sdk(
                "dbfs.get_status", dbfs_path, lambda c: c.dbfs.get_status(dbfs_path)
            )
        return map_info(status, detail=True)

    async def _ls(self, path, detail=True, **kwargs):
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            entries = await self._sdk(
                "dbfs.list", dbfs_path, lambda c: list(c.dbfs.list(dbfs_path))
            )
        return [map_info(i, detail) for i in entries]

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        if (await self._info(path1))["type"] == "directory":
            await self._cp_directory(path2)
            return

        src, dst = parse_dbfs_path(path1), parse_dbfs_path(path2)
        with error_mapping(path1):
            await self._sdk(
                "dbfs.copy",
                src,
                lambda c: c.dbfs.copy(src, dst, recursive=False, overwrite=True),
            )

    async def _mv_file(self, path1, path2, **kwargs):
        """Move a file, or a directory with all in it, with the API's move: one request."""
        src, dst = parse_dbfs_path(path1), parse_dbfs_path(path2)
        with error_mapping(path1):
            await self._sdk("dbfs.move", src, lambda c: c.dbfs.move(src, dst))

    async def _delete(self, path: str, recursive: bool) -> None:
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            # The API's own delete: the SDK's refuses directories unless recursive.
            await self._sdk(
                "dbfs.delete",
                dbfs_path,
                lambda c: DbfsAPI.delete(c.dbfs, dbfs_path, recursive=recursive),
            )

    async def _rm_file(self, path, **kwargs):
        if (await self._info(path))["type"] != "file":
            raise is_a_directory_error(path)
        await self._delete(path, recursive=False)

    async def _rm_tree(self, path):
        """Delete a directory with all in it, by the API's recursive delete: one request."""
        await self._delete(path, recursive=True)

    async def _rmdir(self, path):
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
            await self._sdk(
                "dbfs.mkdirs", dbfs_path, lambda c: c.dbfs.mkdirs(dbfs_path)
            )

    ### File I/O
    async def _check_open(self, path: str, mode: str) -> int | None:
        """Refuse what DBFS cannot open, and tell the size of a file read: one get-status."""
        if "a" in mode or "+" in mode:
            # The API's create truncates or refuses an existing file: nothing appends.
            raise UnsupportedOperation("Append and read/write modes are not supported.")

        if "r" in mode:
            info = await self._info(path)
            if info["type"] == "directory":
                raise is_a_directory_error(path)
            return int(info["size"] or 0)

        try:
            info = await self._info(path)
        except FileNotFoundError:
            return None
        if info["type"] == "directory":
            raise is_a_directory_error(path)
        if "x" in mode:
            # Checked on opening, as Python's files do. Creating with overwrite off checks it again.
            raise file_exists_error(path)
        return None

    def _new_file(
        self,
        path: str,
        mode: str,
        size: int | None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> "DbfsReadableFile | DbfsWritableFile":
        """A file object on `loop`, by default the file system's, its fetches or uploads sharing `io_limit`."""
        loop = loop or self.loop
        if "r" in mode:
            return DbfsReadableFile(
                self, path, loop, size or 0, concurrency_limit=self.io_limit
            )
        return DbfsWritableFile(
            self, path, loop, overwrite="x" not in mode, concurrency_limit=self.io_limit
        )

    async def _open_file(self, path: str, mode: str):
        """Open a file object on the running loop."""
        size = await self._check_open(path, mode)
        return self._new_file(path, mode, size, asyncio.get_running_loop())

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
        size = sync(self.loop, self._check_open, path, mode)
        if autocommit or "r" in mode:
            return self._new_file(path, mode, size)
        # The API's stream makes a file visible as it grows, and replaces the file there when it starts. Holding the
        # last part, as Volumes do, would still show the rest. So a transaction writes to a temporary path, moved into
        # place on commit. The file there is new: it writes to it directly, adding no temporary path of its own.
        transaction = TempPathTransaction(
            path,
            lambda p: self._new_file(p, "wb", None),
            functools.partial(self._commit_move, exclusive="x" in mode),
            lambda p: sync(self.loop, self._delete, p, False),
        )
        return TransactionFile(transaction)

    def _commit_move(self, src: str, dst: str, exclusive: bool) -> None:
        """Move a transaction's temporary file into place.

        The API's move refuses an existing destination. So the file there is deleted first, unless exclusive: two
        requests, not atomic.
        """
        if not exclusive:
            with contextlib.suppress(FileNotFoundError):
                sync(self.loop, self._delete, dst, False)
        sync(self.loop, self._mv_file, src, dst)

    def open(self, path, mode="rb", **kwargs):
        """Open a file in the legacy DBFS.

        Parameters
        ----------
        path : str
            The path to the file in the legacy DBFS.
        mode : str, optional
            The file mode, such as `rb` or `w`. Append and update modes are not supported.
        **kwargs
            Passed to `fsspec.AbstractFileSystem.open`. Options of the other backends, such as
            `notebook_format`, are ignored.
        """
        return super().open(path, mode, **kwargs)


class DbfsReadableFile(AdaptiveReadableFile):
    """A legacy DBFS file read by the API's read, at an offset: requests of 1 MB at most, fetched together.

    The API names no version of a file, so a file replaced while read is not detected.
    """

    min_block_size: int = _MiB // 2
    """The smallest read, the API's default length. Smaller reads fetch it whole."""

    max_block_size: int = _max_request_size
    """The largest read, the API's limit. Larger reads are split, to fetch the parts together."""

    def __init__(
        self, fs: DBFS, path: str, loop: asyncio.AbstractEventLoop, size: int, **kwargs
    ):
        # Kept within the API's limit, whatever the caller asks.
        kwargs["max_block_size"] = min(
            kwargs.get("max_block_size") or self.max_block_size, _max_request_size
        )
        super().__init__(path, loop, size, **kwargs)
        self.fs = fs
        self._dbfs_path = parse_dbfs_path(path)

    async def _fetch_range(self, start: int, end: int) -> bytes:
        chunks = []
        offset = start
        while offset < end:
            length = min(end - offset, _max_request_size)
            with error_mapping(self.path):
                response = await self.fs._blocking(
                    lambda o=offset, n=length: self.fs.client.dbfs.read(
                        self._dbfs_path, offset=o, length=n
                    )
                )
            if not response.bytes_read:
                # The file was truncated meanwhile: the rest is missing.
                break
            chunks.append(base64.b64decode(response.data or ""))
            offset += response.bytes_read
        return b"".join(chunks)


class DbfsWritableFile(AdaptiveWritableFile):
    """A legacy DBFS file written by one put, or by the API's stream: create, add-blocks, close.

    The stream takes blocks one at a time, in order, so the file is `sequential`. Each part is sent as add-blocks of
    1 MB at most, the API's limit. A file of 1 MB at most is sent in one put.

    DBFS cannot abort a stream. Its create replaces the file there, and the file grows with each block. So an abort
    closes the stream and deletes the file: a file written in `wb` mode over another leaves neither.
    """

    min_block_size: int = _max_request_size
    """The smallest part but the last: one add-block."""

    max_block_size: int = _max_request_size
    """The largest part but the last. Parts gain nothing from being larger: each add-block takes 1 MB at most."""

    min_multipart_size: int = _max_request_size
    """The size from which a file is written by a stream. The put's contents take 1 MB at most."""

    sequential: bool = True

    def __init__(
        self,
        fs: DBFS,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        overwrite: bool = True,
        **kwargs,
    ):
        # The put's contents are limited too: a bigger file must be streamed.
        kwargs["min_multipart_size"] = min(
            kwargs.get("min_multipart_size") or self.min_multipart_size,
            _max_request_size,
        )
        super().__init__(path, loop, **kwargs)
        self.fs = fs
        self.overwrite = overwrite
        self._dbfs_path = parse_dbfs_path(path)
        self._handle: int | None = None

    async def _call(self, api: str, func: Any) -> Any:
        with error_mapping(self.path):
            return await self.fs._sdk(api, self._dbfs_path, func)

    async def _upload_all(self, data: bytes) -> None:
        contents = base64.b64encode(data).decode()
        await self._call(
            "dbfs.put",
            lambda c: c.dbfs.put(
                self._dbfs_path, contents=contents, overwrite=self.overwrite
            ),
        )

    async def _start_multipart_upload(self) -> None:
        created = await self._call(
            "dbfs.create",
            lambda c: c.dbfs.create(self._dbfs_path, overwrite=self.overwrite),
        )
        self._handle = created.handle

    async def _upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> None:
        # The last part takes the rest, up to twice the API's limit: it is split.
        view = memoryview(data)
        for offset in range(0, len(view), _max_request_size):
            block = base64.b64encode(view[offset : offset + _max_request_size]).decode()
            await self._call(
                "dbfs.add-block", lambda c, b=block: c.dbfs.add_block(self._handle, b)
            )

    async def _complete_multipart_upload(self, parts: list[Any]) -> None:
        await self._call("dbfs.close", lambda c: c.dbfs.close(self._handle))

    async def _abort_multipart_upload(self) -> None:
        if self._handle is None:
            # The create failed, e.g. on an existing file in x mode: nothing of ours is there.
            return
        handle, self._handle = self._handle, None
        logger.warning("Aborting upload: path=%s", self.path)
        try:
            await self._call("dbfs.close", lambda c: c.dbfs.close(handle))
        finally:
            await self._call(
                "dbfs.delete",
                lambda c: DbfsAPI.delete(c.dbfs, self._dbfs_path, recursive=False),
            )


__all__ = [
    "DBFS",
    "DbfsReadableFile",
    "DbfsWritableFile",
]

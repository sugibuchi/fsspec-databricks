import asyncio
import base64
import contextlib
import functools
import logging
from io import UnsupportedOperation
from typing import Any

from aiohttp import ClientError, ClientMiddlewareType, ClientSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.files import DbfsAPI, FileInfo
from fsspec.asyn import sync
from fsspec_alt_strategies.profile import StorageProfile

from ._contrib.strategy.files.adaptive import (
    AdaptiveReadableFile,
    AdaptiveWritableFile,
)
from ._contrib.strategy.files.fsspec_async import AsyncFileAdapter
from ._contrib.strategy.files.fsspec_transaction import (
    TempPathTransaction,
    TransactionFile,
)
from .base import BaseDatabricksFileSystem
from .error import (
    error_mapping,
    file_exists_error,
    is_a_directory_error,
    not_a_directory_error,
    not_empty_error,
)
from .http_client import (
    AioHttpSessionSupport,
    HttpFileMixin,
    HttpReadableMixin,
    check_response,
    response_error,
    workspace_authenticator,
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


class DBFS(AioHttpSessionSupport, BaseDatabricksFileSystem):
    """Legacy DBFS file system, on the DBFS API.

    An async fsspec file system: its synchronous methods run on fsspec's event loop, and the SDK's calls in
    threads. Its recursive operations are the tree strategies of fsspec-alt-strategies, with the API's own
    recursive move and delete, as `storage_profile` says. Its files call the API with aiohttp.
    """

    storage_profile = StorageProfile(
        fs_type="tree",
        parents_required=False,
        implicit_directories=False,
        renames_files=True,
        renames_directories=True,
        deletes_directories=True,
    )
    """What the legacy DBFS API does.

    - It lists one directory per request, so the tree strategies suit it.
    - Writing a file creates its missing parent directories, as `mkdirs` does.
    - Directories are objects of their own: an empty one exists.
    - Its move renames a file, or a directory with all in it, in one request: `_mv_file` and `_mv_tree`.
    - Its delete deletes a directory with all in it, in one request: `_rm_tree`, which asks the path first, as the
      delete succeeds on a missing one.
    """

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
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
            The maximum number of concurrent reads and uploads of the files opened, shared by all of them.
            Defaults to 8.
        **storage_options
            Standard fsspec storage options, such as `asynchronous` and `loop`.
        """
        self._init_session()
        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            **storage_options,
        )
        # The workspace setting, asked only by `legacy_dbfs_enabled`: a runtime attribute, so a copy asks again.
        self._legacy_dbfs_enabled: bool | None = None

    @property
    def legacy_dbfs_enabled(self) -> bool:
        """Whether legacy DBFS is enabled in the workspace.

        The first call asks the workspace setting `disable_legacy_dbfs` and blocks until it answers. Never call it
        on the event loop. Nothing else asks it: on a workspace without legacy DBFS, each request fails with the
        API's 403, raised as a `PermissionError`.
        """
        if self._legacy_dbfs_enabled is None:
            setting = self.client.workspace_settings_v2.get_public_workspace_setting(
                "disable_legacy_dbfs"
            )
            self._legacy_dbfs_enabled = not setting.effective_boolean_val.value
        return self._legacy_dbfs_enabled

    ### File/directory information
    async def _info(self, path, **kwargs):
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            status = await self._run_in_thread(self.client.dbfs.get_status, dbfs_path)
        return map_info(status, detail=True)

    async def _list(self, path, **kwargs):
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            async for entry in self._iter_in_thread(self.client.dbfs.list, dbfs_path):
                yield map_info(entry, True)

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        if (await self._info(path1))["type"] == "directory":
            await self._cp_directory(path2)
            return

        src, dst = parse_dbfs_path(path1), parse_dbfs_path(path2)
        with error_mapping(path1):
            await self._run_in_thread(
                self.client.dbfs.copy, src, dst, recursive=False, overwrite=True
            )

    async def _mv_file(self, path1, path2, **kwargs):
        """Move a file, or a directory with all in it, with the API's move: one request."""
        src, dst = parse_dbfs_path(path1), parse_dbfs_path(path2)
        with error_mapping(path1):
            await self._run_in_thread(self.client.dbfs.move, src, dst)

    async def _mv_tree(self, path1, path2):
        """Move a directory with all in it: `_mv_file`, as the API's move takes both."""
        await self._mv_file(path1, path2)

    async def _delete(self, path: str, recursive: bool) -> None:
        dbfs_path = parse_dbfs_path(path)
        with error_mapping(path):
            # The API's own delete: the SDK's refuses directories unless recursive.
            await self._run_in_thread(
                DbfsAPI.delete, self.client.dbfs, dbfs_path, recursive=recursive
            )

    async def _rm_file(self, path, **kwargs):
        if (await self._info(path))["type"] != "file":
            raise is_a_directory_error(path)
        await self._delete(path, recursive=False)

    async def _rm_tree(self, path):
        """Delete a directory with all in it, by the API's recursive delete.

        The API's delete succeeds on a missing path, so the path is asked for first: a missing one raises
        FileNotFoundError, as `_rm_tree`'s contract says.
        """
        await self._info(path)
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
            await self._run_in_thread(self.client.dbfs.mkdirs, dbfs_path)

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

    async def _open_file(self, path: str, mode: str):
        """Open a file object on the running loop, its fetches or uploads sharing `io_limit`."""
        size = await self._check_open(path, mode)
        loop = asyncio.get_running_loop()
        session = await self._get_session()
        auth = workspace_authenticator(self.config)
        if "r" in mode:
            return DbfsReadableFile(
                path,
                loop,
                size or 0,
                session=session,
                base_url=self.config.host,
                auth=auth,
                concurrency_limit=self.io_limit,
            )
        else:
            return DbfsWritableFile(
                path,
                loop,
                session=session,
                base_url=self.config.host,
                auth=auth,
                overwrite="x" not in mode,
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
        # The API's stream makes a file visible as it grows, and replaces the file there when it starts. Holding the
        # last part, as Volumes do, would still show the rest. So a transaction writes to a temporary path, moved into
        # place on commit. The file there is new: it writes to it directly, adding no temporary path of its own.
        transaction = TempPathTransaction(
            path,
            lambda p: sync(self.loop, self._open_file, p, "wb"),
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
            Passed to `fsspec.AbstractFileSystem.open`, e.g. `compression`, `encoding`, `errors` and
            `newline`. Options of the other backends, such as `notebook_format`, are ignored.
        """
        return super().open(path, mode, **kwargs)


class DbfsReadableFile(HttpReadableMixin, AdaptiveReadableFile):
    """A legacy DBFS file read by the API's read, at an offset: requests of 1 MB at most, fetched together.

    The API names no version of a file, so a file replaced while read is not detected.
    """

    min_block_size: int = _MiB // 2
    """The smallest read, the API's default length. Smaller reads fetch it whole."""

    max_block_size: int = _max_request_size
    """The largest read, the API's limit. Larger reads are split, to fetch the parts together."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        size: int,
        *,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        **kwargs,
    ):
        # Kept within the API's limit, whatever the caller asks.
        kwargs["max_block_size"] = min(
            kwargs.get("max_block_size") or self.max_block_size, _max_request_size
        )
        super().__init__(path, loop, size, **kwargs)
        self._init_http(session, base_url, auth)
        self._dbfs_path = parse_dbfs_path(path)

    async def _request_range(self, start: int, end: int) -> bytes:
        chunks = []
        offset = start
        while offset < end:
            length = min(end - offset, _max_request_size)
            async with self._session.get(
                f"{self._base_url}/api/2.0/dbfs/read",
                params={"path": self._dbfs_path, "offset": offset, "length": length},
                middlewares=(self._auth,),
                timeout=self._timeout,
            ) as response:
                # Reads of ranges are not logged: a read makes many, and the lines would drown the rest.
                response.raise_for_status()
                result = await response.json()
            bytes_read = result.get("bytes_read") or 0
            if not bytes_read:
                # The file was truncated meanwhile: the rest is missing.
                break
            chunks.append(base64.b64decode(result.get("data") or ""))
            offset += bytes_read
        return b"".join(chunks)


class DbfsWritableFile(HttpFileMixin, AdaptiveWritableFile):
    """A legacy DBFS file written by one put, or by the API's stream: create, add-blocks, close.

    The stream takes blocks one at a time, in order, so the file is `sequential`. Each part is one add-block of
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

    max_request_size: int | None = _max_request_size
    """The API's limit on a put's contents and on each add-block."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        session: ClientSession,
        base_url: str,
        auth: ClientMiddlewareType,
        overwrite: bool = True,
        **kwargs,
    ):
        super().__init__(path, loop, **kwargs)
        self._init_http(session, base_url, auth)
        self.overwrite = overwrite
        self._dbfs_path = parse_dbfs_path(path)
        self._handle: int | None = None

    async def _post(self, endpoint: str, body: dict[str, Any], doing: str) -> Any:
        """Call the API, its errors raised as `OSError`s."""
        try:
            async with self._session.post(
                f"{self._base_url}/api/2.0/dbfs/{endpoint}",
                json=body,
                middlewares=(self._auth,),
                timeout=self._timeout,
            ) as response:
                await check_response(response, self.path, doing)
                return await response.json()
        except (ClientError, TimeoutError) as e:
            raise response_error(self.path, e, doing) from e

    async def _upload_all(self, data: bytes) -> None:
        await self._post(
            "put",
            {
                "path": self._dbfs_path,
                "contents": base64.b64encode(data).decode(),
                "overwrite": self.overwrite,
            },
            "Putting",
        )

    async def _start_multipart_upload(self) -> None:
        created = await self._post(
            "create",
            {"path": self._dbfs_path, "overwrite": self.overwrite},
            "Creating stream",
        )
        self._handle = created["handle"]

    async def _upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> None:
        await self._post(
            "add-block",
            {"handle": self._handle, "data": base64.b64encode(data).decode()},
            f"Adding block {index}",
        )

    async def _complete_multipart_upload(self, parts: list[Any]) -> None:
        await self._post("close", {"handle": self._handle}, "Closing stream")

    async def _abort_multipart_upload(self) -> None:
        if self._handle is None:
            # The create failed, e.g. on an existing file in x mode: nothing of ours is there.
            return
        handle, self._handle = self._handle, None
        logger.warning("Aborting upload: path=%s", self.path)
        try:
            await self._post("close", {"handle": handle}, "Closing stream")
        finally:
            await self._post(
                "delete", {"path": self._dbfs_path, "recursive": False}, "Deleting"
            )


__all__ = [
    "DBFS",
    "DbfsReadableFile",
    "DbfsWritableFile",
]

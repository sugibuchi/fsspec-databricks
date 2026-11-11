import atexit
import contextlib
import logging
import weakref
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from itertools import islice
from typing import Any, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from ._contrib.strategy.common.sync_client import SyncClientSupport
from ._contrib.strategy.tree.filesystem import TreeFileSystem
from ._contrib.strategy.tree.listing import _tree_ils
from .error import file_exists_error, not_a_directory_error
from .path import dbfs_root, scheme, strip_scheme, unstrip_scheme
from .utils import write_mode

T = TypeVar("T")

# The most entries taken from an SDK listing at a time, in one of the threads.
_LISTING_BATCH = 1000

logger = logging.getLogger(__name__)

root_dir_info = {
    "name": dbfs_root,
    "size": None,
    "type": "directory",
    "created": None,
    "modified": None,
    "islink": False,
}


def _cleanup(ref):
    fs = ref()
    if fs is not None:
        fs.close()


def _register_cleanup(fs):
    atexit.register(_cleanup, weakref.ref(fs))


def _next_batch(entries: Iterator[T]) -> list[T]:
    """Take the next entries of an SDK listing, fetching its pages as it comes to them."""
    return list(islice(entries, _LISTING_BATCH))


class DatabricksClientSupport:
    """The Databricks SDK client and configuration of a file system, made on first use.

    A mixin, so that file systems with another fsspec base class share it.
    """

    def _init_client(
        self,
        client: WorkspaceClient | None,
        config: Config | None,
        config_params: dict[str, Any] | None,
    ) -> None:
        self._closed: bool = False

        self._client = client
        self._config = config
        self._config_params = config_params or {}

        self._active_client = self._client
        self._active_config = client.config if client else self._config

    def _close_client(self) -> None:
        self._active_client = None
        self._active_config = None
        self._closed = True

    @property
    def closed(self) -> bool:
        """Whether the file system is closed."""
        return self._closed

    @property
    def config(self) -> Config:
        """The Databricks SDK `Config`, made on first use."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self._active_config is None:
            if self._client is not None:
                self._active_config = self._client.config
            else:
                self._active_config = Config(**self._config_params)
        return self._active_config

    @property
    def client(self):
        """The Databricks SDK `WorkspaceClient`, made on first use."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self._active_client is None:
            if self._client is not None:
                self._active_client = self._client
            else:
                self._active_client = WorkspaceClient(config=self.config)
        return self._active_client


class BaseDatabricksFileSystem(
    DatabricksClientSupport, SyncClientSupport, TreeFileSystem
):
    """Base class of the backends whose API is the Databricks SDK: async, with the tree strategies.

    A subclass implements the primitives of `TreeFileSystem`, with `_list` for `_ils`, and `_create_directory`. The
    SDK's calls block, so they run in threads with `_run_in_thread`, which logs each.
    """

    protocol = scheme
    root_marker = "/"

    operation_concurrency: int = 16
    """The maximum number of concurrent API requests of a recursive operation, such as `rm` or `copy`."""

    io_concurrency: int = 8
    """The maximum number of concurrent transfers of the files opened, shared by all of them."""

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        **storage_options,
    ):
        super().__init__(
            operation_concurrency=operation_concurrency or self.operation_concurrency,
            io_concurrency=io_concurrency or self.io_concurrency,
            # No Databricks file system caches listings.
            use_listings_cache=False,
            **storage_options,
        )
        self._init_client(client, config, config_params)

        _register_cleanup(self)

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)

    def close(self):
        """Close the file system and release any resources."""
        if not self.closed:
            self._close_client()
        # The threads, from SyncClientSupport.
        super().close()

    def __enter__(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        with contextlib.suppress(Exception):
            self.close()

    async def _run_in_thread(self, func: Callable[..., T], /, *args, **kwargs) -> T:
        """Run a blocking call in the file system's own threads, logging it as one request to Databricks.

        The log names the call and its first argument, the path of most SDK calls.
        """
        api = getattr(func, "__qualname__", func)
        path = args[0] if args else None
        try:
            result = await super()._run_in_thread(func, *args, **kwargs)
        except Exception as e:
            logger.debug("%s: path=%s, failed=%s", api, path, type(e).__name__)
            raise
        logger.debug("%s: path=%s, ok", api, path)
        return result

    async def _iter_in_thread(
        self, func: Callable[..., Iterable[T]], /, *args, **kwargs
    ) -> AsyncIterator[T]:
        """Iterate an SDK listing, a batch at a time, each taken in the file system's own threads.

        The SDK's listings are iterators fetching a page as they come to it. So the entries are yielded as the pages
        come, and a folder of many entries is never held whole: the strategies act on the first page while the next
        ones are fetched.
        """
        entries = iter(await self._run_in_thread(func, *args, **kwargs))
        while batch := await self._run_in_thread(_next_batch, entries):
            for entry in batch:
                yield entry

    def created(self, path):
        return self.info(path)["created"]

    def modified(self, path):
        return self.info(path)["modified"]

    def _ils(self, path, recursive=False, **kwargs):
        """List a directory a page at a time, or all below it with `recursive`, a listing per directory.

        No Databricks API lists a tree in one request: the library's `_tree_ils` lists its directories, several at a
        time, each by `_list`.
        """
        if recursive:
            return _tree_ils(self, path, recursive=False, **kwargs)
        else:
            return self._list(path, **kwargs)

    async def _list(self, path: str, **kwargs) -> AsyncIterator[dict[str, Any]]:
        """List one directory, a page at a time, as the SDK's listing fetches them."""
        raise NotImplementedError
        yield {}  # An async generator, as a subclass's is.

    async def _cp_file_entry(self, entry, path2, **kwargs):
        # The copies need nothing of the entry but its path.
        await self._cp_file(entry["name"], path2, **kwargs)

    async def _mv_file_entry(self, entry, path2, **kwargs):
        await self._mv_file(entry["name"], path2, **kwargs)

    async def _mv_file(self, path1, path2, **kwargs):
        """Move a file: a copy, then a delete, where the API has no move."""
        await self._cp_file(path1, path2, **kwargs)
        await self._rm_file(path1)

    async def _create_directory(self, path: str) -> None:
        """Create a directory and its parents with the API."""
        raise NotImplementedError

    async def _cp_directory(self, path2: str) -> None:
        """Copy a directory: create the destination, as directories have no content."""
        try:
            info = await self._info(path2)
        except FileNotFoundError:
            await self._makedirs(path2, exist_ok=True)
            return
        if info["type"] != "directory":
            raise file_exists_error(path2)

    async def _mkdir(self, path, create_parents=True, **kwargs):
        parent = self._parent(path)
        if not await self._isdir(parent):
            # Tell a missing parent from a file.
            try:
                await self._info(parent)
            except FileNotFoundError:
                if not create_parents:
                    raise
            else:
                raise not_a_directory_error(parent)

        if await self._exists(path):
            raise file_exists_error(path)

        # The APIs create the parents too.
        await self._create_directory(path)

    async def _makedirs(self, path, exist_ok=False):
        try:
            info = await self._info(path)
        except FileNotFoundError:
            await self._create_directory(path)
            return
        if info["type"] != "directory":
            raise not_a_directory_error(path)
        if not exist_ok:
            raise file_exists_error(path)

    def open(self, path, mode="rb", **kwargs):
        """Open a file, writing text with "\\n" newlines by default."""
        # Text is any mode without "b": "w" as well as "wt".
        if "b" not in mode and write_mode(mode) and kwargs.get("newline") is None:
            kwargs["newline"] = "\n"
        return super().open(path, mode, **kwargs)


__all__ = [
    "BaseDatabricksFileSystem",
    "DatabricksClientSupport",
    "root_dir_info",
]

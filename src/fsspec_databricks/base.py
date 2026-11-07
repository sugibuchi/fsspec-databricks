import asyncio
import atexit
import contextlib
import logging
import weakref
from collections.abc import Callable
from typing import Any, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from fsspec.spec import AbstractFileSystem

from ._contrib.strategy.tree.filesystem import TreeFileSystem
from .error import file_exists_error, not_a_directory_error
from .file import write_mode
from .path import dbfs_root, scheme, strip_scheme, unstrip_scheme

T = TypeVar("T")

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
        """Check if the file system is closed.

        Returns
        -------
        bool
            `True` if the file system is closed, `False` otherwise.
        """
        return self._closed

    @property
    def config(self) -> Config:
        """Databricks Workspace API client configuration.

        Returns
        -------
        Config
            The Databricks configuration object.
        """
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
        """Client for accessing the Databricks Workspace API.

        Returns
        -------
        WorkspaceClient
            The Databricks SDK WorkspaceClient object.
        """
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self._active_client is None:
            if self._client is not None:
                self._active_client = self._client
            else:
                self._active_client = WorkspaceClient(config=self.config)
        return self._active_client


class AbstractDatabricksFileSystem(DatabricksClientSupport, AbstractFileSystem):
    """Base class of the synchronous Databricks file systems: the client, closing and the protocol."""

    protocol = scheme
    root_marker = "/"

    log = logging.getLogger(__name__)

    verbose_debug_log: bool = False
    """Whether to enable verbose debug logging for file system operations."""

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        """Initialize the file system.

        Parameters
        ----------
        client : WorkspaceClient, optional
            A Databricks SDK WorkspaceClient object.
        config : Config, optional
            A Databricks SDK Config object.
        config_params : dict[str, Any], optional
            A dictionary of parameters to create a Databricks SDK Config object.
        verbose_debug_log : bool, optional
            Whether to enable verbose debug logging for file system operations.
        **storage_options
            Standard fsspec storage options.
        """
        # Directory listing cache is explicitly disabled for all Databricks file systems.
        super().__init__(use_listings_cache=False, **storage_options)

        if verbose_debug_log is not None:
            self.verbose_debug_log = verbose_debug_log

        self._init_client(client, config, config_params)

        _register_cleanup(self)
        self.log.debug("Opened: %s", self)

    def close(self):
        """Close the file system and release any resources."""
        if not self.closed:
            self._close_client()
            self.log.debug("Closed: %s", self)

    def __enter__(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)


class DatabricksTreeFileSystem(DatabricksClientSupport, TreeFileSystem):
    """Base class of the backends whose API is the Databricks SDK: async, with the tree strategies.

    A subclass implements the primitives of `TreeFileSystem` and `_create_directory`. The SDK's calls block, so
    they run in threads with `_sdk`.
    """

    protocol = scheme
    root_marker = "/"

    operation_concurrency: int = 16
    """The maximum number of concurrent API requests of a recursive operation, such as listings and deletions."""

    io_concurrency: int = 8
    """The maximum number of concurrent file transfers, shared by all the files opened."""

    verbose_debug_log: bool = False
    """Whether to enable verbose debug logging for file system operations."""

    log = logging.getLogger(__name__)

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        super().__init__(
            operation_concurrency=operation_concurrency or self.operation_concurrency,
            io_concurrency=io_concurrency or self.io_concurrency,
            # Directory listing cache is explicitly disabled for all Databricks file systems.
            use_listings_cache=False,
            **storage_options,
        )
        self._init_client(client, config, config_params)

        if verbose_debug_log is not None:
            self.verbose_debug_log = verbose_debug_log

        _register_cleanup(self)
        self.log.debug("Opened: %s", self)

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)

    def close(self):
        """Close the file system and release any resources."""
        if not self.closed:
            self._close_client()
            self.log.debug("Closed: %s", self)

    def __enter__(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        with contextlib.suppress(Exception):
            self.close()

    async def _sdk(self, func: Callable[[WorkspaceClient], T]) -> T:
        """Call the blocking Databricks SDK in a thread, so that the event loop goes on."""
        return await asyncio.to_thread(lambda: func(self.client))

    def created(self, path):
        return self.info(path)["created"]

    def modified(self, path):
        return self.info(path)["modified"]

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

        # The APIs create the parents too
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

    def open(self, path, mode="rb", **kwargs):
        """Open a file, writing text with "\\n" newlines by default."""
        if "t" in mode and write_mode(mode) and kwargs.get("newline") is None:
            kwargs["newline"] = "\n"
        return super().open(path, mode, **kwargs)


__all__ = [
    "AbstractDatabricksFileSystem",
    "DatabricksClientSupport",
    "DatabricksTreeFileSystem",
    "root_dir_info",
]

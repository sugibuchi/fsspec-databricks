import atexit
import logging
import weakref
from datetime import datetime
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from fsspec.spec import AbstractFileSystem

from .error import (
    file_exists_error,
    is_a_directory_error,
    not_a_directory_error,
)
from .file import write_mode
from .path import dbfs_root, scheme, strip_scheme, unstrip_scheme

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


class AbstractDatabricksFileSystem(AbstractFileSystem):
    """Abstract base class for Databricks file systems."""

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
        *args
            Standard fsspec filesystem options.
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

        self._closed: bool = False

        self._client = client
        self._config = config
        self._config_params = config_params or {}

        self.__client = self._client
        self.__config = client.config if client else self._config

        _register_cleanup(self)
        self.log.debug("Opened: %s", self)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_AbstractDatabricksFileSystem__client"]
        del state["_AbstractDatabricksFileSystem__config"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__client = None
        self.__config = None

    def close(self):
        """Close the file system and release any resources."""
        if not self.closed:
            self.__client = None
            self.__config = None
            self._closed = True
            self.log.debug("Closed: %s", self)

    def __enter__(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

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

        if self.__config is None:
            if self._client is not None:
                self.__config = self._client.config
            else:
                self.__config = Config(**self._config_params)
        return self.__config

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

        if self.__client is None:
            if self._client is not None:
                self.__client = self._client
            else:
                self.__client = WorkspaceClient(config=self.config)
        return self.__client

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)

    ### File/directory information

    def ls(self, path, detail=True, **kwargs) -> list[str] | list[dict[str, Any]]:
        self.log.debug("Listing directory: path=%s, detail=%s", path, detail)

    def info(self, path, **kwargs) -> dict[str, Any]:
        self.log.debug("Getting info: path=%s", path)

    def exists(self, path, **kwargs) -> bool:
        self.log.debug("Checking existence: path=%s", path)

    def created(self, path) -> datetime | None:
        return self.info(path)["created"]

    def modified(self, path) -> datetime | None:
        return self.info(path)["modified"]

    ### File/directory operations
    def cp_file(self, path1, path2, **kwargs):
        self.log.debug("Copying file: path1=%s, path2=%s", path1, path2)

    def _rm(self, path):
        # Implement only the precondition check here
        # Actual operations must be implemented by subclasses
        self.log.debug("Deleting file: path=%s", path)

        info = self.info(path)
        if info["type"] != "file":
            raise is_a_directory_error(path)

    def mkdir(self, path, create_parents=True, **kwargs):
        # Implement only the precondition check here
        # Actual operations must be implemented by subclasses
        self.log.debug(
            "Making directory: path=%s, create_parents=%s", path, create_parents
        )

        try:
            info = self.info(self._parent(path))
            if info["type"] != "directory":
                raise not_a_directory_error(self._parent(path))
        except FileNotFoundError:
            if create_parents:
                return
            else:
                raise

        if self.exists(path):
            raise file_exists_error(path)

    def makedirs(self, path, exist_ok=False):
        # Implement only the precondition check here
        # Actual operations must be implemented by subclasses
        self.log.debug("Making directories: path=%s, exist_ok=%s", path, exist_ok)

        try:
            info = self.info(path)
            if info["type"] == "directory":
                if exist_ok:
                    return
                else:
                    raise file_exists_error(path)
            else:
                raise not_a_directory_error(path)
        except FileNotFoundError:
            pass

    def rmdir(self, path):
        # Implement only the precondition check here
        # Actual operations must be implemented by subclasses
        self.log.debug("Deleting directory: path=%s", path)

        info = self.info(path)
        if info["type"] != "directory":
            raise not_a_directory_error(path)

    def open(
        self,
        path,
        mode="rb",
        block_size: int | None = None,
        compression: str | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        **kwargs,
    ):
        """Open a file in the Unity Catalog Volume file system.

        Parameters
        ----------
        path : str
            The path to the file in the Unity Catalog Volume file system.
        mode : str, optional
            The file mode like `rb`, `w`.
        block_size : int, optional
            The block size to use when accessing data in the file.
        max_size : int, optional
            The maximum size of the file to read or write.
        compression : str, optional
            If given, open file using compression codec. Can either be a compression
            name (a key in ``fsspec.compression.compr``) or "infer" to guess the
            compression from the filename suffix.
        encoding : str, optional
            The encoding to use when opening the file. Only applicable for text mode.
            See `io.TextIOWrapper <https://docs.python.org/3/library/io.html#buffered-streams>`_.
        errors : str, optional
            The error handling strategy to use when opening the file in text mode. Only applicable for text mode.
            See `io.TextIOWrapper <https://docs.python.org/3/library/io.html#buffered-streams>`_.
        newline : str, optional
            The newline handling strategy to use when opening the file in text mode. Only applicable for text mode.
            See `io.TextIOWrapper <https://docs.python.org/3/library/io.html#buffered-streams>`_.
        **kwargs
            Additional keyword arguments to pass to `AbstractBufferedFile` constructor.
        """
        self.log.debug("Opening file: path=%s, mode=%s", path, mode)

        if "t" in mode and write_mode(mode):
            newline = newline or "\n"

        return super().open(
            path=path,
            mode=mode,
            block_size=block_size,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )


__all__ = [
    "AbstractDatabricksFileSystem",
    "root_dir_info",
]

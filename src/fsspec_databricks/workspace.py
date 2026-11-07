import asyncio
import errno
import io
from io import UnsupportedOperation
from typing import Any, Literal

from databricks.sdk.errors import BadRequest
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)
from fsspec_alt_strategies_common.profile import StorageProfile

from ._contrib.strategy.files.fsspec_async import SyncFileAdapter
from ._contrib.strategy.files.fsspec_transaction import FileTransaction, TransactionFile
from .base import DatabricksTreeFileSystem, root_dir_info
from .error import (
    error_mapping,
    file_exists_error,
    file_not_found_error,
    is_a_directory_error,
    not_a_directory_error,
    not_empty_error,
)
from .file import write_mode
from .path import dbfs_root, parse_workspace_path, workspace_root
from .utils import to_datetime, value_of

workspace_root_info = {
    "name": workspace_root,
    "size": None,
    "type": "directory",
    "created": None,
    "modified": None,
}

NotebookFormat = Literal[
    "SOURCE",
    "HTML",
    "JUPYTER",
    "DBC",
    "R_MARKDOWN",
]


def _map_info(info: ObjectInfo, detail: bool) -> dict[str, Any] | str:
    """Map a Databricks SDK `ObjectInfo` instance to a dictionary in the format used by `fsspec.info()`."""

    if detail:
        return {
            "name": f"/Workspace{info.path}",
            "size": info.size,
            "type": "directory" if info.object_type == ObjectType.DIRECTORY else "file",
            "created": to_datetime(info.created_at),
            "modified": to_datetime(info.modified_at),
            "islink": False,
            "object_type": info.object_type.value if info.object_type else None,
            "language": info.language.value if info.language else None,
            "object_id": info.object_id,
            "resource_id": info.resource_id,
        }
    else:
        return f"/Workspace{info.path}"


def _parse(path: str) -> str:
    """`parse_workspace_path`, raising FileNotFoundError for paths outside /Workspace."""
    try:
        return parse_workspace_path(path)
    except ValueError:
        raise file_not_found_error(path) from None


class WorkspaceFileSystem(DatabricksTreeFileSystem):
    """Databricks Workspace file system.

    An async fsspec file system: its synchronous methods run on fsspec's event loop, and the SDK's calls in
    threads. Its recursive operations are the tree strategies of fsspec-alt-strategies. A recursive `rm`
    deletes each directory by the API's own recursive delete, `_rm_tree`.
    """

    storage_profile = StorageProfile(implicit_folders=False, renames_files=False)
    """Directories are objects of their own, and the Workspace API has no move."""

    def __init__(
        self,
        client=None,
        config=None,
        config_params: dict[str, Any] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        **storage_options,
    ):
        """Initialize the Workspace file system.

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
            The maximum number of concurrent downloads and uploads of the files opened.
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
        if self._strip_protocol(path) == dbfs_root:
            return dict(root_dir_info)

        workspace_path = _parse(path)
        with error_mapping(path):
            status = await self._sdk(
                "workspace.get_status",
                workspace_path,
                lambda c: c.workspace.get_status(workspace_path),
            )
        return _map_info(status, detail=True)

    async def _ls(self, path, detail=True, **kwargs):
        if self._strip_protocol(path) == dbfs_root:
            return [dict(workspace_root_info)] if detail else [workspace_root]

        workspace_path = _parse(path)
        with error_mapping(path):
            entries = await self._sdk(
                "workspace.list",
                workspace_path,
                lambda c: list(c.workspace.list(workspace_path)),
            )
        return [_map_info(i, detail) for i in entries]

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        source_info = await self._info(path1)
        if source_info["type"] == "directory":
            await self._cp_directory(path2)
            return

        if source_info["object_type"] == "NOTEBOOK":
            export_format = ExportFormat.SOURCE
            import_format = ImportFormat.SOURCE
            language = value_of(source_info["language"], Language)
        else:
            export_format = ExportFormat.AUTO
            import_format = ImportFormat.RAW
            language = None

        file_path1 = _parse(path1)
        file_path2 = _parse(path2)

        def download(c):
            with c.workspace.download(file_path1, format=export_format) as f:
                return f.read()

        with error_mapping(path1):
            data = await self._sdk("workspace.download", file_path1, download)
        with error_mapping(path2):
            await self._sdk(
                "workspace.upload",
                file_path2,
                lambda c: c.workspace.upload(
                    file_path2,
                    data,
                    format=import_format,
                    language=language,
                    overwrite=True,
                ),
            )

    async def _delete(self, path: str, recursive: bool) -> None:
        workspace_path = _parse(path)
        with error_mapping(path):
            await self._sdk(
                "workspace.delete",
                workspace_path,
                lambda c: c.workspace.delete(workspace_path, recursive=recursive),
            )

    async def _rm_file(self, path, **kwargs):
        # Checked first: the API deletes an empty directory as a file. The
        # strategies give only files, but fsspec's non-recursive rm may not.
        if (await self._info(path))["type"] != "file":
            raise is_a_directory_error(path)
        await self._delete(path, recursive=False)

    async def _rm_tree(self, path):
        """Delete a directory with all in it, by the API's recursive delete: one request."""
        await self._delete(path, recursive=True)

    async def _rmdir(self, path):
        if (await self._info(path))["type"] != "directory":
            raise not_a_directory_error(path)

        with error_mapping(path):
            try:
                await self._delete(path, recursive=False)
            except BadRequest as e:
                if "is not empty" in str(e):
                    raise not_empty_error(path) from e
                raise

    async def _create_directory(self, path):
        workspace_path = _parse(path)
        with error_mapping(path):
            await self._sdk(
                "workspace.mkdirs",
                workspace_path,
                lambda c: c.workspace.mkdirs(workspace_path),
            )

    ### File I/O
    def _file_info(self, path: str) -> dict[str, Any] | None:
        """The info of the file at the path, None if missing, with a blocking SDK call."""
        if self._strip_protocol(path) == dbfs_root:
            raise is_a_directory_error(path)
        try:
            with error_mapping(path):
                info = _map_info(
                    self.client.workspace.get_status(_parse(path)), detail=True
                )
        except FileNotFoundError:
            return None
        if info["type"] == "directory":
            raise is_a_directory_error(path)
        return info

    def _open_file(
        self,
        path: str,
        mode: str,
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
    ) -> "WorkspaceFile":
        """Open a file object with the formats of the object at the path.

        It calls the blocking SDK directly: never call it on the loop.
        """
        if "+" in mode:
            raise UnsupportedOperation(f"Update mode (+) is not supported: {mode}")
        info = self._file_info(path)

        if "r" in mode and info is None:
            raise file_not_found_error(path)

        if "x" in mode and info is not None:
            raise file_exists_error(path)

        if info and info["object_type"] == "NOTEBOOK":
            # File exists and is a notebook
            export_format = (
                value_of(notebook_format, ExportFormat) or ExportFormat.SOURCE
            )
            import_format = (
                value_of(notebook_format, ImportFormat) or ImportFormat.SOURCE
            )
            language = value_of(language or info["language"], Language)
        elif info:
            # File exists and is not a notebook
            if notebook_format is not None:
                raise ValueError(
                    f"notebook_format is not applicable for non-notebook object: {path}"
                )
            export_format = ExportFormat.AUTO
            import_format = ImportFormat.RAW
            language = None
        else:
            # File does not exist yet
            if write_mode(mode) and notebook_format and not language:
                raise ValueError(
                    f"language must be specified when writing a notebook: {path}"
                )

            export_format = value_of(notebook_format, ExportFormat) or ExportFormat.AUTO
            import_format = value_of(notebook_format, ImportFormat) or ImportFormat.RAW
            language = value_of(language, Language)

        return WorkspaceFile(
            self,
            path,
            mode,
            export_format=export_format,
            import_format=import_format,
            language=language,
        )

    async def open_async(
        self,
        path,
        mode="rb",
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
        **kwargs,
    ):
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError("Only binary mode without compression is supported")
        # Opening downloads the file: seek and tell then need no request on the loop.
        f = await asyncio.to_thread(
            self._open_file, path, mode, notebook_format, language
        )
        return SyncFileAdapter(f)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
        **kwargs,
    ):
        f = self._open_file(path, mode, notebook_format, language)
        if autocommit or "r" in mode:
            return f
        return TransactionFile(_WorkspaceTransaction(f))

    def open(
        self,
        path,
        mode="rb",
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
        **kwargs,
    ):
        """Open a file in the Databricks Workspace file system.

        Parameters
        ----------
        path : str
            The path to the file in the Databricks Workspace.
        mode : str, optional
            The file mode like `rb`, `w`. Update modes (`+`) are not supported.
        notebook_format : NotebookFormat, optional
            The format to use when reading or writing a notebook. Applies only to notebook objects.
        language : Language, str, optional
            Language of the notebook to read or write. Applies only to notebook objects.
        **kwargs
            Passed to `fsspec.AbstractFileSystem.open`, e.g. `compression`, `encoding`, `errors` and `newline`.
        """
        return super().open(
            path, mode, notebook_format=notebook_format, language=language, **kwargs
        )


class WorkspaceFile(io.BytesIO):
    """A Workspace file or notebook, downloaded whole and uploaded whole, as the Workspace API transfers them.

    A synchronous file calling the SDK directly, with no event loop:

    - Opening for reading or appending downloads the whole file into memory.
    - Closing uploads it whole, if written, or if the file is new.
    - Leaving a `with` block by an exception, `discard`, or the garbage collector uploads nothing.
    - Exclusive creation uploads without overwriting, so that the API refuses a file created meanwhile.
    - Files are at most `max_size` bytes: beyond it, OSError with `errno.EFBIG`.
    """

    max_size = 10 * 1024 * 1024
    """The Workspace API's limit."""

    def __init__(
        self,
        fs: WorkspaceFileSystem,
        path: str,
        mode: str = "rb",
        *,
        export_format: ExportFormat | None = None,
        import_format: ImportFormat | None = None,
        language: Language | None = None,
    ):
        super().__init__()
        # Set first, so that closing a file whose download failed uploads nothing.
        self._discarded = True
        self.fs = fs
        self.path = path
        self.mode = mode
        self.export_format = export_format
        self.import_format = import_format
        self.language = language
        self._kind = mode.replace("b", "").replace("t", "")
        if self._kind not in ("r", "w", "a", "x"):
            raise ValueError(f"Unsupported mode: {mode!r}")
        # Whether closing uploads: a new file is uploaded even if empty.
        self._changed = self._kind in "wx"
        if self._kind in "ra":
            try:
                super().write(self._download())
            except FileNotFoundError:
                if self._kind == "r":
                    raise
                self._changed = True  # Appending creates the file.
            self.seek(0, io.SEEK_END if self._kind == "a" else io.SEEK_SET)
        self._discarded = False

    def _download(self) -> bytes:
        workspace_path = parse_workspace_path(self.path)
        with (
            error_mapping(self.path),
            self.fs.client.workspace.download(
                workspace_path, format=self.export_format
            ) as src,
        ):
            data = src.read()
        _check_size(len(data), self.max_size, self.path)
        return data

    def _upload(self) -> None:
        workspace_path = parse_workspace_path(self.path)
        with error_mapping(self.path):
            self.fs.client.workspace.upload(
                workspace_path,
                self.getvalue(),
                format=self.import_format,
                language=self.language,
                # The API refuses to overwrite: no other client's file is lost.
                overwrite=self._kind != "x",
            )

    @property
    def size(self) -> int:
        """The file's size so far."""
        return self.getbuffer().nbytes

    def readable(self) -> bool:
        self._check_open()
        return self._kind == "r"

    def writable(self) -> bool:
        self._check_open()
        return self._kind != "r"

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file.")

    def read(self, size: int | None = -1) -> bytes:
        if not self.readable():
            raise UnsupportedOperation(f"File not open for reading: {self.path}")
        return super().read(size)

    def read1(self, size: int | None = -1) -> bytes:
        return self.read(size)

    def readinto(self, buffer: Any) -> int:
        if not self.readable():
            raise UnsupportedOperation(f"File not open for reading: {self.path}")
        return super().readinto(buffer)

    def readline(self, size: int | None = -1) -> bytes:
        if not self.readable():
            raise UnsupportedOperation(f"File not open for reading: {self.path}")
        return super().readline(size)

    def write(self, data: Any) -> int:
        if not self.writable():
            raise UnsupportedOperation(f"File not open for writing: {self.path}")
        if self._kind == "a":
            self.seek(0, io.SEEK_END)
        size = memoryview(data).nbytes
        _check_size(max(self.tell() + size, self.size), self.max_size, self.path)
        self._changed = True
        return super().write(data)

    def writelines(self, lines: Any) -> None:
        for line in lines:
            self.write(line)

    def truncate(self, size: int | None = None) -> int:
        if not self.writable():
            raise UnsupportedOperation(f"File not open for writing: {self.path}")
        self._changed = True
        return super().truncate(size)

    def discard(self) -> None:
        """Upload nothing on closing: the changes are lost."""
        self._discarded = True

    def close(self) -> None:
        if self.closed:
            return
        try:
            if not self._discarded and self._changed:
                self._upload()
        finally:
            super().close()

    def __exit__(self, exc_type: object, *exc_info: object) -> None:
        if exc_type is not None:
            self.discard()
        self.close()

    def __del__(self) -> None:
        # A file the garbage collector finds open was abandoned: nothing is uploaded.
        self._discarded = True
        super().__del__()


class _WorkspaceTransaction(FileTransaction):
    """A `WorkspaceFile` in a transaction: its buffer is uploaded on commit, as closing does, or dropped on discard."""

    def __init__(self, file: WorkspaceFile):
        super().__init__(file.path)
        self.file = file

    def start_staging(self) -> WorkspaceFile:
        return self.file

    def end_staging(self) -> None:
        pass  # The buffer stays open, for commit to upload.

    def commit(self) -> None:
        self.file.close()

    def discard(self) -> None:
        self.file.discard()
        self.file.close()


def _check_size(size: int, max_size: int, path: str) -> None:
    if size > max_size:
        raise OSError(errno.EFBIG, f"File too large, over {max_size} bytes", path)


__all__ = [
    "NotebookFormat",
    "WorkspaceFile",
    "WorkspaceFileSystem",
    "workspace_root_info",
]

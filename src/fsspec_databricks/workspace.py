import asyncio
import logging
from glob import has_magic
from io import UnsupportedOperation
from typing import IO, Any, Literal

from databricks.sdk.errors import BadRequest
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)
from fsspec.asyn import sync
from fsspec_alt_strategies_common.profile import StorageProfile

from ._contrib.strategy.files.buffered import LocallyBufferedFile
from ._contrib.strategy.files.fsspec_async import AsyncFileAdapter
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
    threads. Its recursive operations are the tree strategies of fsspec-alt-strategies, except a recursive `rm`
    without glob patterns, which is the API's own recursive delete.
    """

    storage_profile = StorageProfile(implicit_folders=False, renames_files=False)
    """Directories are objects of their own, and the Workspace API has no move."""

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

        if self._strip_protocol(path) == dbfs_root:
            return dict(root_dir_info)

        workspace_path = _parse(path)
        with error_mapping(path):
            status = await self._sdk(lambda c: c.workspace.get_status(workspace_path))
        return _map_info(status, detail=True)

    async def _ls(self, path, detail=True, **kwargs):
        self.log.debug("Listing directory: path=%s, detail=%s", path, detail)

        if self._strip_protocol(path) == dbfs_root:
            return [dict(workspace_root_info)] if detail else [workspace_root]

        workspace_path = _parse(path)
        with error_mapping(path):
            entries = await self._sdk(lambda c: list(c.workspace.list(workspace_path)))
        return [_map_info(i, detail) for i in entries]

    ### File/directory operations
    async def _cp_file(self, path1, path2, **kwargs):
        self.log.debug("Copying file: path1=%s, path2=%s", path1, path2)

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

        def copy(c):
            with (
                error_mapping(path1),
                c.workspace.download(file_path1, format=export_format) as f,
            ):
                data = f.read()
            with error_mapping(path2):
                c.workspace.upload(
                    file_path2,
                    data,
                    format=import_format,
                    language=language,
                    overwrite=True,
                )

        await self._sdk(copy)

    async def _delete(self, path: str, recursive: bool) -> None:
        workspace_path = _parse(path)
        with error_mapping(path):
            await self._sdk(
                lambda c: c.workspace.delete(workspace_path, recursive=recursive)
            )

    async def _rm_file(self, path, **kwargs):
        self.log.debug("Deleting file: path=%s", path)

        # Checked first: the API deletes an empty directory as a file.
        if (await self._info(path))["type"] != "file":
            raise is_a_directory_error(path)
        await self._delete(path, recursive=False)

    async def _rm(
        self, path, recursive=False, batch_size=None, maxdepth=None, **kwargs
    ):
        self.log.debug(
            "Deleting path: path=%s, recursive=%s, maxdepth=%s",
            path,
            recursive,
            maxdepth,
        )

        paths = [path] if isinstance(path, str) else list(path)
        if maxdepth is not None or any(
            has_magic(self._strip_protocol(p)) for p in paths
        ):
            await super()._rm(path, recursive, batch_size, maxdepth, **kwargs)
            return

        # The API's own recursive delete: one request per path.
        for p in paths:
            if recursive:
                await self._delete(p, recursive=True)
            else:
                await self._rm_file(p)

    async def _rmdir(self, path):
        self.log.debug("Deleting directory: path=%s", path)

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
            await self._sdk(lambda c: c.workspace.mkdirs(workspace_path))

    ### File I/O
    async def _open_file(
        self,
        path: str,
        mode: str,
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
    ) -> "WorkspaceFile":
        """Open a file object on the running loop, with the formats of the object at the path."""
        self.log.debug("Opening file: path=%s, mode=%s", path, mode)

        if "+" in mode:
            raise UnsupportedOperation(f"Update mode (+) is not supported: {mode}")

        try:
            info = await self._info(path)
            if info["type"] == "directory":
                raise is_a_directory_error(path)
        except FileNotFoundError:
            info = None

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
        f = await self._open_file(path, mode, notebook_format, language)
        # Downloaded now, so that seek and tell need no request on the loop.
        await f.aload()
        return AsyncFileAdapter(f)

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
        return sync(self.loop, self._open_file, path, mode, notebook_format, language)

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


class WorkspaceFile(LocallyBufferedFile):
    """A Workspace file or notebook, downloaded whole and uploaded whole, as the Workspace API transfers them."""

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
        super().__init__(
            path,
            asyncio.get_running_loop(),
            mode,
            concurrency_limit=fs.io_limit,
        )
        self.fs = fs
        self.export_format = export_format
        self.import_format = import_format
        self.language = language

    async def _download(self, target: IO[bytes]) -> None:
        workspace_path = parse_workspace_path(self.path)

        def download(c):
            with c.workspace.download(workspace_path, format=self.export_format) as src:
                return src.read()

        with error_mapping(self.path):
            data = await self.fs._sdk(download)
        target.write(data)

    async def _send(self, source: IO[bytes], overwrite: bool) -> None:
        workspace_path = parse_workspace_path(self.path)
        data = source.read()
        with error_mapping(self.path):
            await self.fs._sdk(
                lambda c: c.workspace.upload(
                    workspace_path,
                    data,
                    format=self.import_format,
                    language=self.language,
                    overwrite=overwrite,
                )
            )

    async def _upload(self, source: IO[bytes]) -> None:
        await self._send(source, overwrite=True)

    async def _create(self, source: IO[bytes]) -> None:
        # The API refuses to overwrite: no other client's file is lost.
        await self._send(source, overwrite=False)

    async def _exists(self) -> bool:
        return await self.fs._exists(self.path)


__all__ = [
    "NotebookFormat",
    "WorkspaceFile",
    "WorkspaceFileSystem",
    "workspace_root_info",
]

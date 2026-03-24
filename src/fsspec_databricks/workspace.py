import glob
import logging
import os
from io import BytesIO
from typing import Any, Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import BadRequest, ResourceDoesNotExist
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

from .base import AbstractDatabricksFileSystem, root_dir_info
from .error import (
    error_mapping,
    file_exists_error,
    file_not_found_error,
    is_a_directory_error,
    not_empty_error,
)
from .file import MemoryCachedFile, write_mode
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


class WorkspaceFileSystem(AbstractDatabricksFileSystem):
    """Databricks Workspace file system."""

    log = logging.getLogger(__name__)

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        """Initialize the Workspace file system."""
        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            verbose_debug_log=verbose_debug_log,
            **storage_options,
        )

    ### File/directory information
    def info(self, path, **kwargs):
        super().info(path=path, **kwargs)

        stripped = self._strip_protocol(path)
        if stripped == dbfs_root:
            return root_dir_info

        try:
            workspace_path = parse_workspace_path(path)
        except ValueError:
            raise file_not_found_error(path) from None

        with error_mapping(path):
            return _map_info(
                self.client.workspace.get_status(workspace_path),
                detail=True,
            )

    def ls(self, path, detail=True, **kwargs):
        super().ls(path=path, detail=detail, **kwargs)

        stripped = self._strip_protocol(path)
        if stripped == dbfs_root:
            return [workspace_root_info] if detail else [workspace_root]

        try:
            workspace_path = parse_workspace_path(path)
        except ValueError:
            raise file_not_found_error(path) from None

        with error_mapping(path):
            return [
                _map_info(i, detail) for i in self.client.workspace.list(workspace_path)
            ]

    def exists(self, path, **kwargs):
        try:
            with error_mapping(path):
                self.client.workspace.get_status(parse_workspace_path(path))
                return True
        except FileNotFoundError:
            return False

    def created(self, path):
        return self.info(path)["created"]

    def modified(self, path):
        return self.info(path)["modified"]

    ### File/directory operations
    def cp_file(self, path1, path2, **kwargs):
        # The precondition check is implemented in the base class.
        super().cp_file(path1=path1, path2=path2, **kwargs)

        file_path1 = parse_workspace_path(path1)
        file_path2 = parse_workspace_path(path2)

        source_info = self.info(path1)
        if source_info["type"] == "directory":
            try:
                dest_info = self.info(path2)
                if dest_info["type"] == "directory":
                    return
                else:
                    raise file_exists_error(path2)
            except FileNotFoundError:
                self.mkdirs(path2)
                return

        if source_info["object_type"] == "NOTEBOOK":
            export_format = ExportFormat.SOURCE
            import_format = ImportFormat.SOURCE
            language = Language(source_info["language"])
        else:
            export_format = ExportFormat.AUTO
            import_format = ImportFormat.RAW
            language = None

        with (
            error_mapping(path1),
            self.client.workspace.download(
                file_path1,
                format=export_format,
            ) as f,
            error_mapping(path2),
        ):
            self.client.workspace.upload(
                file_path2,
                f,
                format=import_format,
                language=language,
            )

    def _rm(self, path):
        # The precondition check is implemented in the base class.
        super()._rm(path=path)

        with error_mapping(path):
            self.client.workspace.delete(parse_workspace_path(path), recursive=False)

    def rm(self, path, recursive=False, maxdepth=None):
        self.log.debug(
            "Deleting path: path=%s, recursive=%s, maxdepth=%s",
            path,
            recursive,
            maxdepth,
        )

        if isinstance(path, (str, os.PathLike)):
            self.rm([path], recursive=recursive, maxdepth=maxdepth)
        else:
            paths = [(p, parse_workspace_path(p)) for p in path]
            if all(not glob.has_magic(parsed) for p, parsed in paths):
                # Utilize the API's native recursive delete if there are no glob patterns.
                with error_mapping(path):
                    for p, parsed in paths:
                        if not recursive and self.isdir(p):
                            raise is_a_directory_error(p)
                        self.client.workspace.delete(parsed, recursive=recursive)
            else:
                super().rm(path, recursive=recursive, maxdepth=maxdepth)

    def mkdir(self, path, create_parents=True, **kwargs):
        # The precondition check is implemented in the base class.
        super().mkdir(path=path, create_parents=create_parents, **kwargs)

        with error_mapping(path):
            self.client.workspace.mkdirs(parse_workspace_path(path))

    def makedirs(self, path, exist_ok=False):
        # The precondition check is implemented in the base class.
        super().makedirs(path=path, exist_ok=exist_ok)

        with error_mapping(path):
            self.client.workspace.mkdirs(parse_workspace_path(path))

    def rmdir(self, path):
        # The precondition check is implemented in the base class.
        super().rmdir(path=path)

        with error_mapping(path):
            try:
                self.client.workspace.delete(
                    parse_workspace_path(path),
                    recursive=False,
                )
            except BadRequest as e:
                if "is not empty" in e.args[0]:
                    raise not_empty_error(path) from e
                else:
                    raise

    def _open(
        self,
        path,
        mode="rb",
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
        **kwargs,
    ):
        if "+" in mode:
            raise ValueError(f"Update mode (+) is not supported: {mode}")

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

        if info and info["object_type"] == "NOTEBOOK":
            # File exists and is a notebook
            export_format = (
                value_of(notebook_format, ExportFormat) or ExportFormat.SOURCE
            )
            import_format = (
                value_of(notebook_format, ImportFormat) or ImportFormat.SOURCE
            )
            language = value_of(language or info["language"], Language)
        elif info and info["object_type"] != "NOTEBOOK":
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
            client=self.client,
            path=path,
            export_format=export_format,
            import_format=import_format,
            language=language,
            mode=mode,
            verbose_debug_log=self.verbose_debug_log,
        )

    def open(
        self,
        path,
        mode="rb",
        notebook_format: NotebookFormat | None = None,
        language: Language | str | None = None,
        compression: str | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        **kwargs,
    ):
        """Open a file in the Databricks Workspace file system.

        Parameters
        ----------
        path : str
            The path to the file in the Databricks Workspace.
        mode : str, optional
            The file mode like `rb`, `w`.
        notebook_format : NotebookFormat, optional
            The format to use when reading or writing a notebook. Applies only to notebook objects.
        language : Language, str, optional
            Language of the notebook to read or write. Applies only to notebook objects.
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
        return super().open(
            path,
            mode=mode,
            notebook_format=notebook_format,
            language=language,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )


class WorkspaceFile(MemoryCachedFile):
    log = WorkspaceFileSystem.log

    max_size = 10 * 1024 * 1024  # 10 MB
    block_size = 10 * 1024 * 1024  # 10 MB

    def __init__(
        self,
        path: str,
        client: WorkspaceClient,
        export_format: ExportFormat | None = None,
        import_format: ImportFormat | None = None,
        language: Language | None = None,
        mode="rb",
        verbose_debug_log: bool | None = None,
    ):
        self._client = client
        self.export_format = export_format
        self.import_format = import_format
        self.language = language

        super().__init__(
            path=path,
            mode=mode,
            verbose_debug_log=verbose_debug_log,
        )

    def _remote_file_exists(self):
        try:
            self._client.workspace.get_status(parse_workspace_path(self.path))
            return True
        except ResourceDoesNotExist:
            return False

    def _remote_file_download(self):
        with (
            error_mapping(self.path),
            self._client.workspace.download(
                path=parse_workspace_path(self.path), format=self.export_format
            ) as src,
        ):
            return BytesIO(src.read())

    def _remote_file_upload(self, data):
        if hasattr(data, "read"):
            data = data.read()

        with error_mapping(self.path):
            self._client.workspace.upload(
                path=parse_workspace_path(self.path),
                content=data,
                format=self.import_format,
                language=self.language,
                overwrite=True,
            )


__all__ = [
    "WorkspaceFileSystem",
    "workspace_root_info",
]

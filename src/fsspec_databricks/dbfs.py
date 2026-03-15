import glob
import logging
from os import PathLike
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.files import FileInfo

from .base import AbstractDatabricksFileSystem
from .error import error_mapping, file_exists_error, is_a_directory_error
from .path import parse_dbfs_path
from .utils import to_datetime


def map_info(info: FileInfo, detail: bool) -> dict[str, Any] | str:
    if detail:
        return {
            "name": info.path.rstrip("/"),
            "size": info.file_size,
            "type": "directory" if info.is_dir else "file",
            "created": None,
            "modified": to_datetime(info.modification_time),
            "islink": False,
        }
    else:
        return info.path.rstrip("/")


class DBFS(AbstractDatabricksFileSystem):
    """Databricks DBFS API."""

    log = logging.getLogger(__name__)

    def __init__(
        self,
        client: WorkspaceClient | None = None,
        config: Config | None = None,
        config_params: dict[str, Any] | None = None,
        verbose_debug_log: bool | None = None,
        **storage_options,
    ):
        """Initialize the DBFS ."""
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

        with error_mapping(path):
            return map_info(
                self.client.dbfs.get_status(parse_dbfs_path(path)), detail=True
            )

    def ls(self, path, detail=True, **kwargs):
        super().ls(path=path, detail=detail, **kwargs)

        with error_mapping(path):
            return [
                map_info(i, detail)
                for i in self.client.dbfs.list(parse_dbfs_path(path))
            ]

    def exists(self, path, **kwargs):
        super().exists(path=path, **kwargs)

        with error_mapping(path):
            return self.client.dbfs.exists(parse_dbfs_path(path))

    ### File/directory operations
    def cp_file(self, path1, path2, **kwargs):
        # The precondition check is implemented in the base class.
        super().cp_file(path1=path1, path2=path2, **kwargs)

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

        with error_mapping(path1):
            self.client.dbfs.copy(
                src=parse_dbfs_path(path1),
                dst=parse_dbfs_path(path2),
                recursive=False,
                overwrite=True,
            )

    def _rm(self, path):
        # The precondition check is implemented in the base class.
        super()._rm(path=path)

        with error_mapping(path):
            self.client.dbfs.delete(parse_dbfs_path(path), recursive=False)

    def rm(self, path, recursive=False, maxdepth=None):
        self.log.debug(
            "Deleting path: path=%s, recursive=%s, maxdepth=%s",
            path,
            recursive,
            maxdepth,
        )

        if isinstance(path, (str, PathLike)):
            self.rm([path], recursive=recursive, maxdepth=maxdepth)
        else:
            paths = [(p, parse_dbfs_path(p)) for p in path]
            if all(not glob.has_magic(parsed) for p, parsed in paths):
                # Utilize the API's native recursive delete if there are no glob patterns.
                with error_mapping(path):
                    for p, parsed in paths:
                        if not recursive and self.isdir(p):
                            raise is_a_directory_error(p)
                        self.client.dbfs.delete(parsed, recursive=recursive)
            else:
                super().rm(path, recursive=recursive, maxdepth=maxdepth)

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        self.log.debug(
            "Moving path: path1=%s, path2=%s, recursive=%s, maxdepth=%s",
            path1,
            path2,
            recursive,
            maxdepth,
        )

        if (
            not isinstance(path1, list)
            and not isinstance(path2, list)
            and not glob.has_magic(path1)
            and not glob.has_magic(path2)
        ):
            # Use the native move API.
            with error_mapping(path1):
                self.client.dbfs.move_(
                    src=parse_dbfs_path(path1),
                    dst=parse_dbfs_path(path2),
                    recursive=recursive,
                    overwrite=False,
                )
            return

        super().mv(
            path1=path1, path2=path2, recursive=recursive, maxdepth=maxdepth, **kwargs
        )

    def mkdir(self, path, create_parents=True, **kwargs):
        # The precondition check is implemented in the base class.
        super().mkdir(path=path, create_parents=create_parents, **kwargs)

        with error_mapping(path):
            self.client.dbfs.mkdirs(parse_dbfs_path(path))

    def makedirs(self, path, exist_ok=False):
        # The precondition check is implemented in the base class.
        super().makedirs(path=path, exist_ok=exist_ok)

        with error_mapping(path):
            self.client.dbfs.mkdirs(parse_dbfs_path(path))

    def rmdir(self, path):
        # The precondition check is implemented in the base class.
        super().rmdir(path=path)

        with error_mapping(path):
            self.client.dbfs.delete(parse_dbfs_path(path), recursive=True)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        self.log.debug("Opening file: path=%s, mode=%s", path, mode)

        with error_mapping():
            return self.client.dbfs.open(
                parse_dbfs_path(path),
                read="r" in mode,
                write="w" in mode,
                overwrite=True,
            )


__all__ = [
    "DBFS",
]

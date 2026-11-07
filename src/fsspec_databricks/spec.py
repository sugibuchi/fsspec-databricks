import atexit
import json
import os
import shutil
from typing import Any

from databricks.sdk import AuthorizationDetail, CredentialsStrategy, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.workspace import Language
from fsspec.spec import AbstractFileSystem

from .base import AbstractDatabricksFileSystem, root_dir_info
from .dbfs import DBFS
from .error import file_exists_error, file_not_found_error
from .local import DatabricksLocalFileSystem
from .path import (
    FileSystemType,
    from_local_path,
    fs_type,
    scheme,
    to_local_path,
    volumes_root,
    workspace_root,
)
from .volume import VolumeFileSystem, volumes_root_info
from .workspace import NotebookFormat, WorkspaceFileSystem, workspace_root_info

_cross_fs_copy_chunk_size = 8 * 1024 * 1024
"""The bytes copied at a time between file systems, e.g. from Workspace to Volume."""


class DatabricksFileSystem(AbstractDatabricksFileSystem):
    """File system for accessing Databricks Workspace files, Unity Catalog Volumes, and the legacy DBFS."""

    protocol = scheme

    use_local_fs_in_workspace: bool = True
    """Whether to access files from the local file system rather than the remote Databricks API when running within a Databricks workspace. Defaults to True."""

    legacy_dbfs_local_root: str = "/dbfs"
    """The local path where Databricks mounts the legacy DBFS when running within a Databricks workspace."""

    def __init__(
        self,
        # Parameters for Databricks WorkspaceClient
        host: str | None = None,
        account_id: str | None = None,
        username: str | None = None,
        password: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token: str | None = None,
        profile: str | None = None,
        config_file: str | None = None,
        group_id: str | None = None,
        azure_workspace_resource_id: str | None = None,
        azure_client_secret: str | None = None,
        azure_client_id: str | None = None,
        azure_tenant_id: str | None = None,
        azure_environment: str | None = None,
        auth_type: str | None = None,
        cluster_id: str | None = None,
        google_credentials: str | None = None,
        google_service_account: str | None = None,
        workspace_id: int | None = None,
        debug_truncate_bytes: int | None = None,
        debug_headers: bool | None = None,
        product="unknown",
        product_version="0.0.0",
        credentials_strategy: CredentialsStrategy | None = None,
        credentials_provider: CredentialsStrategy | None = None,
        token_audience: str | None = None,
        config: Config | None = None,
        scopes: list[str] | None = None,
        authorization_details: list[AuthorizationDetail] | None = None,
        custom_headers: dict | None = None,
        # Additional parameters
        client: WorkspaceClient | None = None,
        ## Options for Unity Catalog Volume file system
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        ## Fallback to local file system when running on Databricks
        use_local_fs_in_workspace: bool | None = None,
        **storage_options,
    ):
        """Initialize the DatabricksFileSystem.

        Parameters
        ----------
        *args
            Standard fsspec filesystem options.
        host
            See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.
        account_id
            See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.
        token
            See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.
        username
            See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.
        password
            See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.
        client_id
            (Service principal OAuth only) The client ID you were assigned when creating your service principal.
        client_secret
            (Service principal OAuth only) The client secret you generated when creating your service principal.
        azure_workspace_resource_id
            See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.
        azure_client_secret
            See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.
        azure_client_id
            See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.
        azure_tenant_id
            See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.
        azure_environment
            See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.
        auth_type
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        cluster_id
            The ID of the cluster to connect. See `Compute configuration for Databricks Connect <https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config>`_.
        profile
            See `Overriding .databrickscfg <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#overriding-databrickscfg>`_.
        config_file
            See `Overriding .databrickscfg <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#overriding-databrickscfg>`_.
        group_id
            The ID of the account group to authenticate as. See the Databricks SDK `Config` class.
        workspace_id
            The ID of the workspace to connect. Required on unified hosts serving multiple workspaces. See the Databricks SDK `Config` class.
        custom_headers
            Custom HTTP headers to add to every request sent by the Databricks SDK.
        debug_headers
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        debug_truncate_bytes
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        config
            An optional pre-configured Databricks SDK `Config` object. If provided, it will be used for authentication.
        client
            An optional pre-configured Databricks SDK `WorkspaceClient` object. If provided, it will be used for accessing the Databricks Workspace API.
        operation_concurrency : int, optional
            The maximum number of concurrent API requests of a recursive operation on Unity Catalog Volumes.
        io_concurrency : int, optional
            The maximum number of concurrent range reads and part uploads of Unity Catalog Volume files.
        use_local_fs_in_workspace
            Access files from the local file system rather than the remote Databricks API when running within a Databricks workspace.
        **storage_options
            Standard fsspec storage options.
        """
        config_params = {
            "host": host,
            "account_id": account_id,
            "username": username,
            "password": password,
            "client_id": client_id,
            "client_secret": client_secret,
            "token": token,
            "profile": profile,
            "config_file": config_file,
            "group_id": group_id,
            "azure_workspace_resource_id": azure_workspace_resource_id,
            "azure_client_secret": azure_client_secret,
            "azure_client_id": azure_client_id,
            "azure_tenant_id": azure_tenant_id,
            "azure_environment": azure_environment,
            "auth_type": auth_type,
            "cluster_id": cluster_id,
            "google_credentials": google_credentials,
            "google_service_account": google_service_account,
            "workspace_id": workspace_id,
            "credentials_strategy": credentials_strategy,
            "credentials_provider": credentials_provider,
            "debug_truncate_bytes": debug_truncate_bytes,
            "debug_headers": debug_headers,
            "product": product,
            "product_version": product_version,
            "token_audience": token_audience,
            "scopes": scopes,
            "authorization_details": (
                json.dumps([detail.as_dict() for detail in authorization_details])
                if authorization_details
                else None
            ),
            "custom_headers": custom_headers,
        }

        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            **storage_options,
        )

        self._storage_options = storage_options

        self._operation_concurrency = operation_concurrency
        self._io_concurrency = io_concurrency
        if use_local_fs_in_workspace is not None:
            self.use_local_fs_in_workspace = use_local_fs_in_workspace

        self.__local_fs: DatabricksLocalFileSystem | None = None
        self.__workspace_fs: WorkspaceFileSystem | None = None
        self.__volume_fs: VolumeFileSystem | None = None
        self.__legacy_fs: DBFS | None = None

        self._running_in_workspace: bool | None = None
        self._legacy_dbfs_enabled: bool | None = None

        atexit.register(self.close)

    def close(self):
        """Close the file system and all underlying file system instances."""
        try:
            errors = []

            def _close(fs):
                try:
                    if fs:
                        fs.close()
                except Exception as e:
                    errors.append(e)

            _close(self.__workspace_fs)
            _close(self.__volume_fs)
            _close(self.__legacy_fs)

            if errors:
                raise RuntimeError(
                    f"Errors occurred while closing file system: {errors}"
                )
        finally:
            self.__workspace_fs = None
            self.__volume_fs = None
            self.__legacy_fs = None
            self.__local_fs = None
            super().close()

    @property
    def _volume_fs(self) -> VolumeFileSystem:
        """Get or create the UnityCatalogVolumeFileSystem instance."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__volume_fs is None:
            self.__volume_fs = VolumeFileSystem(
                client=self.client,
                config=self.config,
                operation_concurrency=self._operation_concurrency,
                io_concurrency=self._io_concurrency,
                **self._storage_options,
            )
        return self.__volume_fs

    @property
    def _workspace_fs(self) -> WorkspaceFileSystem:
        """Get or create the DatabricksWorkspaceFileSystem instance."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__workspace_fs is None:
            self.__workspace_fs = WorkspaceFileSystem(
                client=self.client,
                config=self.config,
                operation_concurrency=self._operation_concurrency,
                io_concurrency=self._io_concurrency,
                **self._storage_options,
            )
        return self.__workspace_fs

    @property
    def _legacy_fs(self) -> DBFS:
        """Get or create the LegacyDBFS instance."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__legacy_fs is None:
            self.__legacy_fs = DBFS(
                client=self.client,
                config=self.config,
                operation_concurrency=self._operation_concurrency,
                io_concurrency=self._io_concurrency,
                **self._storage_options,
            )
        return self.__legacy_fs

    @property
    def _local_fs(self) -> DatabricksLocalFileSystem:
        """Get or create the local file system instance."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__local_fs is None:
            self.__local_fs = DatabricksLocalFileSystem(**self._storage_options)
        return self.__local_fs

    @property
    def running_in_workspace(self) -> bool:
        """Whether the code is running within a Databricks workspace."""
        if self._running_in_workspace is None:
            self._running_in_workspace = "DATABRICKS_RUNTIME_VERSION" in os.environ
        return self._running_in_workspace

    @property
    def legacy_dbfs_enabled(self) -> bool:
        """Whether access to the legacy DBFS file system is enabled.

        With the local file system, it is enabled when its local mount exists, without calling the API.
        """
        if self._legacy_dbfs_enabled is None:
            if self._uses_local_fs:
                self._legacy_dbfs_enabled = os.path.isdir(self.legacy_dbfs_local_root)
            else:
                disabled = (
                    self.client.workspace_settings_v2.get_public_workspace_setting(
                        "disable_legacy_dbfs"
                    ).effective_boolean_val.value
                )
                self._legacy_dbfs_enabled = not disabled

        return self._legacy_dbfs_enabled

    @property
    def _uses_local_fs(self) -> bool:
        """Whether files are accessed from the local file system."""
        return self.running_in_workspace and self.use_local_fs_in_workspace

    def _resolve(self, path: str) -> tuple[AbstractFileSystem, str]:
        """Return the file system for the path, and the path to pass to it."""
        if self._uses_local_fs:
            return self._local_fs, to_local_path(path, self.legacy_dbfs_local_root)

        t = fs_type(path)
        if t == FileSystemType.Workspace:
            return self._workspace_fs, path
        elif t == FileSystemType.Volume:
            return self._volume_fs, path
        elif t == FileSystemType.Legacy and self.legacy_dbfs_enabled:
            return self._legacy_fs, path
        else:
            raise file_not_found_error(path)

    def _from_local_info(self, info: dict) -> dict:
        """Map the name in a local file's info back to its POSIX path in Databricks."""
        if self._uses_local_fs:
            return {
                **info,
                "name": from_local_path(info["name"], self.legacy_dbfs_local_root),
            }
        else:
            return info

    def _from_local_name(self, name: str) -> str:
        if self._uses_local_fs:
            return from_local_path(name, self.legacy_dbfs_local_root)
        return name

    def _from_local_found(self, found: Any, detail: bool) -> Any:
        """Map the names found by `find` or `glob` back, as `_from_local_info` does."""
        if not self._uses_local_fs:
            return found
        if detail:
            return {
                self._from_local_name(k): self._from_local_info(v)
                for k, v in found.items()
            }
        return [self._from_local_name(n) for n in found]

    def _backend(self, path: str) -> tuple[AbstractFileSystem, str] | None:
        """The backend serving a path or glob pattern, with the path to pass to it.

        None for the root, or a pattern matching there, as the root spans the backends.
        """
        stripped = self._strip_protocol(path)
        magic = [i for i in (stripped.find(c) for c in "*?[") if i >= 0]
        base = stripped[: min(magic)].rsplit("/", 1)[0] if magic else stripped
        if base.rstrip("/") == "":
            return None
        fs, _ = self._resolve(base)
        _, p = self._resolve(path)
        return fs, p

    def _same_backend(self, path1, path2) -> tuple[AbstractFileSystem, str, str] | None:
        """The backend serving both paths, with the paths to pass to it: None if there is none."""
        if not isinstance(path1, str) or not isinstance(path2, str):
            return None
        b1, b2 = self._backend(path1), self._backend(path2)
        if b1 is None or b2 is None or b1[0] is not b2[0]:
            return None
        return b1[0], b1[1], b2[1]

    def _cp_directory(self, path2):
        """Copy a directory: create the destination, as directories have no content."""
        try:
            info = self.info(path2)
        except FileNotFoundError:
            self.makedirs(path2, exist_ok=True)
            return
        if info["type"] != "directory":
            raise file_exists_error(path2)

    ### Recursive operations, run by the backend of their path, with its strategies
    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        backend = self._backend(path)
        if backend is None:
            return super().find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
            )
        fs, p = backend
        found = fs.find(
            p, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
        )
        return self._from_local_found(found, detail)

    def glob(self, path, maxdepth=None, **kwargs):
        backend = self._backend(path)
        if backend is None:
            return super().glob(path, maxdepth=maxdepth, **kwargs)
        fs, p = backend
        found = fs.glob(p, maxdepth=maxdepth, **kwargs)
        return self._from_local_found(found, kwargs.get("detail", False))

    def du(self, path, total=True, maxdepth=None, withdirs=False, **kwargs):
        backend = self._backend(path)
        if backend is None:
            return super().du(
                path, total=total, maxdepth=maxdepth, withdirs=withdirs, **kwargs
            )
        fs, p = backend
        sizes = fs.du(p, total=total, maxdepth=maxdepth, withdirs=withdirs, **kwargs)
        if total:
            return sizes
        return {self._from_local_name(k): v for k, v in sizes.items()}

    def walk(self, path, maxdepth=None, topdown=True, on_error="omit", **kwargs):
        backend = self._backend(path)
        if backend is None:
            yield from super().walk(
                path, maxdepth=maxdepth, topdown=topdown, on_error=on_error, **kwargs
            )
            return
        fs, p = backend
        mapped = self._uses_local_fs and kwargs.get("detail")
        for root, dirs, files in fs.walk(
            p, maxdepth=maxdepth, topdown=topdown, on_error=on_error, **kwargs
        ):
            if mapped:
                yield (
                    self._from_local_name(root),
                    {k: self._from_local_info(v) for k, v in dirs.items()},
                    {k: self._from_local_info(v) for k, v in files.items()},
                )
            else:
                yield self._from_local_name(root), dirs, files

    def copy(
        self, path1, path2, recursive=False, maxdepth=None, on_error=None, **kwargs
    ):
        same = self._same_backend(path1, path2)
        if same is None:
            # Between backends: each file is streamed by `cp_file`.
            return super().copy(
                path1,
                path2,
                recursive=recursive,
                maxdepth=maxdepth,
                on_error=on_error,
                **kwargs,
            )
        fs, p1, p2 = same
        return fs.copy(
            p1,
            p2,
            recursive=recursive,
            maxdepth=maxdepth,
            on_error=on_error,
            **kwargs,
        )

    ### File/directory information
    def ls(self, path, detail=True, **kwargs):
        if not self.legacy_dbfs_enabled and self._strip_protocol(path) == "/":
            return (
                [volumes_root_info, workspace_root_info]
                if detail
                else [volumes_root, workspace_root]
            )

        fs, p = self._resolve(path)
        items = fs.ls(p, detail=detail, **kwargs)
        if not self._uses_local_fs:
            return items
        elif detail:
            return [self._from_local_info(i) for i in items]
        else:
            return [from_local_path(i, self.legacy_dbfs_local_root) for i in items]

    def info(self, path, **kwargs):
        if not self.legacy_dbfs_enabled and self._strip_protocol(path) == "/":
            return root_dir_info

        fs, p = self._resolve(path)
        return self._from_local_info(fs.info(p, **kwargs))

    def exists(self, path, **kwargs) -> bool:
        fs, p = self._resolve(path)
        return fs.exists(p, **kwargs)

    def isdir(self, path) -> bool:
        if self._strip_protocol(path) == "/":
            return True

        try:
            fs, p = self._resolve(path)
        except FileNotFoundError:
            return False
        return fs.isdir(p)

    def isfile(self, path) -> bool:
        if self._strip_protocol(path) == "/":
            return False

        try:
            fs, p = self._resolve(path)
        except FileNotFoundError:
            return False
        return fs.isfile(p)

    def created(self, path):
        fs, p = self._resolve(path)
        return fs.created(p)

    def modified(self, path):
        fs, p = self._resolve(path)
        return fs.modified(p)

    ### File/directory operations
    def cp_file(self, path1, path2, **kwargs):
        if fs_type(path1) == fs_type(path2):
            fs, p1 = self._resolve(path1)
            _, p2 = self._resolve(path2)
            fs.cp_file(p1, p2, **kwargs)
        else:
            source_info = self.info(path1)
            if source_info["type"] == "directory":
                self._cp_directory(path2)
                return

            src_fs, src_path = self._resolve(path1)
            dst_fs, dst_path = self._resolve(path2)
            with src_fs.open(src_path, "rb") as src:
                with dst_fs.open(dst_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=_cross_fs_copy_chunk_size)

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        same = self._same_backend(path1, path2)
        if same is not None:
            fs, p1, p2 = same
            fs.mv(
                p1,
                p2,
                recursive=recursive,
                maxdepth=maxdepth,
                **kwargs,
            )
        else:
            super().mv(
                path1,
                path2,
                recursive=recursive,
                maxdepth=maxdepth,
                **kwargs,
            )

    def _rm(self, path):
        fs, p = self._resolve(path)
        fs.rm_file(p)

    def rm(self, path, recursive=False, maxdepth=None):
        if not isinstance(path, str):
            for p in path:
                self.rm(p, recursive=recursive, maxdepth=maxdepth)
            return
        backend = self._backend(path)
        if backend is None:
            super().rm(path, recursive=recursive, maxdepth=maxdepth)
            return
        fs, p = backend
        fs.rm(p, recursive=recursive, maxdepth=maxdepth)

    def mkdir(self, path, create_parents=True, **kwargs):
        fs, p = self._resolve(path)
        fs.mkdir(p, create_parents, **kwargs)

    def makedirs(self, path, exist_ok=False):
        fs, p = self._resolve(path)
        fs.makedirs(p, exist_ok)

    def rmdir(self, path):
        fs, p = self._resolve(path)
        fs.rmdir(p)

    ## File I/O
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
        """Open a file in a file system in Databricks (Workspace, Unity Catalog volume or legacy DBFS).

        Parameters
        ----------
        path : str
            The path to the file in the Unity Catalog Volume file system.
        mode : str, optional
            The file mode like `rb`, `w`.
        notebook_format : NotebookFormat, optional
            The format to use when reading or writing a notebook. Applicable only to Workspace notebooks.
        language : Language, str, optional
            Language of the notebook to read or write. Applicable only to Workspace files.
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
        fs, p = self._resolve(path)
        autocommit = kwargs.pop("autocommit", not self._intrans)
        in_transaction = not autocommit and "r" not in mode
        if in_transaction:
            # The backend's open queues the file in the backend's own transaction, which nothing ends.
            backend_files = fs.transaction.files
        f = fs.open(
            path=p,
            mode=mode,
            notebook_format=notebook_format,
            language=language,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
            autocommit=autocommit,
            **kwargs,
        )
        if in_transaction:
            # The router's transaction commits or discards it, with the files of every other backend.
            self.transaction.files.append(backend_files.pop())
        return f


__all__ = [
    "DatabricksFileSystem",
]

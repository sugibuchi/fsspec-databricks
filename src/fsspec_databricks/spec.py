import atexit
import json
import os
import shutil

from databricks.sdk import AuthorizationDetail, CredentialsStrategy, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from fsspec.spec import AbstractFileSystem

from .base import AbstractDatabricksFileSystem, root_dir_info
from .dbfs import DBFS
from .error import file_exists_error, file_not_found_error
from .local import DatabricksLocalFileSystem
from .path import FileSystemType, fs_type, scheme, volumes_root, workspace_root
from .volume import VolumeFileSystem, volumes_root_info
from .workspace import WorkspaceFileSystem, workspace_root_info


class DatabricksFileSystem(AbstractDatabricksFileSystem):
    """File system for accessing Databricks Workspace files, Unity Catalog Volumes, and the legacy DBFS."""

    protocol = scheme

    volume_fs_max_read_concurrency: int = 24
    """The maximum number of concurrent file read operations on a Unity Catalog Volume file."""

    volume_fs_min_read_block_size: int = 1024 * 1024
    """The minimum data size to read for each read operation on a Unity Catalog Volume file."""

    volume_fs_max_read_block_size: int = 4 * 1024 * 1024
    """The maximum data size to read for each read operation on a Unity Catalog Volume file."""

    volume_fs_max_write_concurrency: int = 24
    """The maximum number of concurrent file write operations on a Unity Catalog Volume file."""

    volume_fs_min_write_block_size: int = 5 * 1024 * 1024
    """The minimum data size to write for each write operation on a Unity Catalog Volume file."""

    volume_fs_max_write_block_size: int = 16 * 1024 * 1024
    """The maximum data size to write for each write operation on a Unity Catalog Volume file."""

    volume_min_multipart_upload_size: int = 5 * 1024 * 1024
    """The minimum file size to use multipart upload for uploading files to Unity Catalog Volume."""

    use_local_fs_in_workspace: bool = True
    """Whether to access files from the local file system rather than the remote Databricks API when running within a Databricks workspace. Defaults to True."""

    cross_fs_copy_block_size: int = 1024 * 1024
    """The block size to use when copying files between different file systems (e.g. from Workspace to Volume). """

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
        azure_workspace_resource_id: str | None = None,
        azure_client_secret: str | None = None,
        azure_client_id: str | None = None,
        azure_tenant_id: str | None = None,
        azure_environment: str | None = None,
        auth_type: str | None = None,
        cluster_id: str | None = None,
        google_credentials: str | None = None,
        google_service_account: str | None = None,
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
        # Additional parameters
        client: WorkspaceClient | None = None,
        ## Options for Unity Catalog Volume file system
        volume_fs_max_read_concurrency: int | None = None,
        volume_fs_min_read_block_size: int | None = None,
        volume_fs_max_read_block_size: int | None = None,
        volume_fs_max_write_concurrency: int | None = None,
        volume_fs_min_write_block_size: int | None = None,
        volume_fs_max_write_block_size: int | None = None,
        volume_min_multipart_upload_size: int | None = None,
        ## Fallback to local file system when running on Databricks
        use_local_fs_in_workspace: bool | None = None,
        ## Logging option
        verbose_debug_log: bool | None = None,
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
        debug_headers
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        debug_truncate_bytes
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        config
            An optional pre-configured Databricks SDK `Config` object. If provided, it will be used for authentication.
        client
            An optional pre-configured Databricks SDK `WorkspaceClient` object. If provided, it will be used for accessing the Databricks Workspace API.
        volume_fs_max_read_concurrency : int, optional
            The maximum number of concurrent file read operations on a Unity Catalog Volume file.
        volume_fs_min_read_block_size : int, optional
            The minimum data size to read for each read operation on a Unity Catalog Volume file.
        volume_fs_max_read_block_size : int, optional
            The maximum data size to read for each read operation on a Unity Catalog Volume file.
        volume_fs_max_write_concurrency : int, optional
            The maximum number of concurrent file write operations on a Unity Catalog Volume file.
        volume_fs_min_write_block_size : int, optional
            The minimum data size to write for each write operation on a Unity Catalog Volume file.
        volume_fs_max_write_block_size : int, optional
            The maximum data size to write for each write operation on a Unity Catalog Volume file.
        volume_min_multipart_upload_size:
            The minimum file size to use multipart upload for uploading files to Unity Catalog Volume.
        use_local_fs_in_workspace
            Access files from the local file system rather than the remote Databricks API when running within a Databricks workspace.
        verbose_debug_log : bool, optional
            Whether to enable verbose debug logging for file system operations.
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
            "azure_workspace_resource_id": azure_workspace_resource_id,
            "azure_client_secret": azure_client_secret,
            "azure_client_id": azure_client_id,
            "azure_tenant_id": azure_tenant_id,
            "azure_environment": azure_environment,
            "auth_type": auth_type,
            "cluster_id": cluster_id,
            "google_credentials": google_credentials,
            "google_service_account": google_service_account,
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
        }

        super().__init__(
            client=client,
            config=config,
            config_params=config_params,
            verbose_debug_log=verbose_debug_log,
            **storage_options,
        )

        self._storage_options = storage_options

        if volume_fs_max_read_concurrency is not None:
            self.volume_fs_max_read_concurrency = volume_fs_max_read_concurrency
        if volume_fs_min_read_block_size is not None:
            self.volume_fs_min_read_block_size = volume_fs_min_read_block_size
        if volume_fs_max_read_block_size is not None:
            self.volume_fs_max_read_block_size = volume_fs_max_read_block_size

        if volume_fs_max_write_concurrency is not None:
            self.volume_fs_max_write_concurrency = volume_fs_max_write_concurrency
        if volume_fs_min_write_block_size is not None:
            self.volume_fs_min_write_block_size = volume_fs_min_write_block_size
        if volume_fs_max_write_block_size is not None:
            self.volume_fs_max_write_block_size = volume_fs_max_write_block_size
        if volume_min_multipart_upload_size is not None:
            self.volume_min_multipart_upload_size = volume_min_multipart_upload_size
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

    def __getstate__(self):
        state = super().__getstate__()
        del state["_DatabricksFileSystem__local_fs"]
        del state["_DatabricksFileSystem__workspace_fs"]
        del state["_DatabricksFileSystem__volume_fs"]
        del state["_DatabricksFileSystem__legacy_fs"]
        del state["_running_in_workspace"]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self.__workspace_fs = None
        self.__volume_fs = None
        self.__legacy_fs = None
        self.__local_fs = None
        self._running_in_workspace = None

    @property
    def _volume_fs(self) -> VolumeFileSystem:
        """Get or create the UnityCatalogVolumeFileSystem instance."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__volume_fs is None:
            self.__volume_fs = VolumeFileSystem(
                client=self.client,
                config=self.config,
                max_read_concurrency=self.volume_fs_max_read_concurrency,
                min_read_block_size=self.volume_fs_min_read_block_size,
                max_read_block_size=self.volume_fs_max_read_block_size,
                max_write_concurrency=self.volume_fs_max_write_concurrency,
                min_write_block_size=self.volume_fs_min_write_block_size,
                max_write_block_size=self.volume_fs_max_write_block_size,
                min_multipart_upload_size=self.volume_min_multipart_upload_size,
                verbose_debug_log=self.verbose_debug_log,
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
                verbose_debug_log=self.verbose_debug_log,
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
                verbose_debug_log=self.verbose_debug_log,
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
        """Whether access to the legacy DBFS file system is enabled."""
        if self._legacy_dbfs_enabled is None:
            disabled = self.client.workspace_settings_v2.get_public_workspace_setting(
                "disable_legacy_dbfs"
            ).effective_boolean_val.value
            self._legacy_dbfs_enabled = not disabled

        return self._legacy_dbfs_enabled

    def _get_backend(self, path: str) -> AbstractFileSystem:
        """Determine the appropriate file system for the path."""
        t = fs_type(path)
        if self.running_in_workspace and self.use_local_fs_in_workspace:
            return self._local_fs
        if t == FileSystemType.Workspace:
            return self._workspace_fs
        elif t == FileSystemType.Volume:
            return self._volume_fs
        elif t == FileSystemType.Legacy and self.legacy_dbfs_enabled:
            return self._legacy_fs
        else:
            raise file_not_found_error(path)

    ### File/directory information
    def ls(self, path, detail=True, **kwargs):
        if not self.legacy_dbfs_enabled and self._strip_protocol(path) == "/":
            return (
                [volumes_root_info, workspace_root_info]
                if detail
                else [volumes_root, workspace_root]
            )
        else:
            return self._get_backend(path).ls(path, detail=detail, **kwargs)

    def info(self, path, **kwargs):
        if not self.legacy_dbfs_enabled and self._strip_protocol(path) == "/":
            return root_dir_info

        return self._get_backend(path).info(path, **kwargs)

    def exists(self, path, **kwargs) -> bool:
        return self._get_backend(path).exists(path, **kwargs)

    def created(self, path):
        return self._get_backend(path).created(path)

    def modified(self, path):
        return self._get_backend(path).modified(path)

    ### File/directory operations
    def cp_file(self, path1, path2, **kwargs):
        if fs_type(path1) == fs_type(path2):
            self._get_backend(path1).cp_file(path1, path2, **kwargs)
        else:
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

            with self._get_backend(path1).open(path1, "rb") as src:
                with self._get_backend(path2).open(path2, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=self.cross_fs_copy_block_size)

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        if fs_type(path1) == fs_type(path2):
            self._get_backend(path1).mv(
                path1,
                path2,
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
        self._get_backend(path)._rm(path)

    def rm(self, path, recursive=False, maxdepth=None):
        self._get_backend(path).rm(path, recursive=recursive, maxdepth=maxdepth)

    def mkdir(self, path, create_parents=True, **kwargs):
        self._get_backend(path).mkdir(path, create_parents, **kwargs)

    def makedirs(self, path, exist_ok=False):
        self._get_backend(path).makedirs(path, exist_ok)

    def rmdir(self, path):
        self._get_backend(path).rmdir(path)

    ## File I/O
    def open(
        self,
        path,
        mode="rb",
        export_format: ExportFormat | str | None = None,
        import_format: ImportFormat | str | None = None,
        language: Language | str | None = None,
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        block_size: int | None = None,
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
        export_format : ExportFormat, optional
            The format to use when exporting the file from the workspace. Applicable only to Workspace files.
        import_format : ImportFormat, optional
            The format to use when importing the file to the workspace. Applicable only to Workspace files.
        language : Language, optional
            Language of the notebook to read or write. Applicable only to Workspace files.
        max_concurrency : int, optional
            The maximum number of concurrent file operations. Applicable only to volume files.
        min_block_size : int, optional
            The minimum data size to read or write for each file operation. Applicable only to volume files.
        max_block_size : int, optional
            The maximum data size to read or write for each file operation. Applicable only to volume files.
        block_size : int, optional
            The data size to read or write for each file operation.
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
        return self._get_backend(path).open(
            path=path,
            mode=mode,
            export_format=export_format,
            import_format=import_format,
            language=language,
            max_concurrency=max_concurrency,
            max_block_size=max_block_size,
            min_block_size=min_block_size,
            block_size=block_size,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )


__all__ = [
    "DatabricksFileSystem",
]

import asyncio
import contextlib
import json
import os
from typing import Any

from databricks.sdk import AuthorizationDetail, CredentialsStrategy, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.workspace import Language
from fsspec.asyn import AsyncFileSystem
from fsspec_alt_strategies_common.profile import StorageProfile

from ._contrib.strategy.routing import AsyncRoutingFileSystem
from ._contrib.strategy.tree.filesystem import TreeFileSystem
from ._contrib.strategy.tree.transfer import tree_transfer
from ._contrib.strategy.tree.wrapper import SyncTreeWrapper
from .base import DatabricksClientSupport, _register_cleanup, root_dir_info
from .dbfs import DBFS
from .error import file_not_found_error
from .local import DatabricksLocalFileSystem
from .path import (
    FileSystemType,
    from_local_path,
    fs_type,
    scheme,
    strip_scheme,
    to_local_path,
    unstrip_scheme,
)
from .utils import write_mode
from .volume import VolumeFileSystem, volumes_root_info
from .workspace import NotebookFormat, WorkspaceFileSystem, workspace_root_info


class DatabricksFileSystem(DatabricksClientSupport, AsyncRoutingFileSystem):
    """File system for Unity Catalog Volumes, Workspace files and legacy DBFS.

    It routes each call to the backend serving the path: `VolumeFileSystem`, `WorkspaceFileSystem` or `DBFS`, or
    the local file system inside a workspace.
    """

    protocol = scheme
    root_marker = "/"

    transfer_concurrency: int = 8
    """The files copied between two backends at a time."""

    use_local_fs_in_workspace: bool = True
    """Whether to use the local file system, rather than the Databricks APIs, when running in a Databricks workspace."""

    legacy_dbfs_local_root: str = "/dbfs"
    """The local path where Databricks mounts legacy DBFS in a workspace."""

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
        ## Options of the backends
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        transfer_concurrency: int | None = None,
        ## Fallback to local file system when running on Databricks
        use_local_fs_in_workspace: bool | None = None,
        **storage_options,
    ):
        """Initialize the DatabricksFileSystem.

        The parameters up to `custom_headers` are those of the Databricks SDK's `WorkspaceClient` and `Config`.

        Parameters
        ----------
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
        google_credentials
            See `Google Cloud native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html>`_.
        google_service_account
            See `Google Cloud native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html>`_.
        workspace_id
            The ID of the workspace to connect to. Required on unified hosts serving several workspaces. See the
            Databricks SDK `Config` class.
        debug_truncate_bytes
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        debug_headers
            See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
        product
            The product name sent in the user agent. See the Databricks SDK `Config` class.
        product_version
            The product version sent in the user agent. See the Databricks SDK `Config` class.
        credentials_strategy
            A custom credentials strategy. See the Databricks SDK `Config` class.
        credentials_provider
            The former name of `credentials_strategy`. See the Databricks SDK `Config` class.
        token_audience
            The audience of OIDC tokens. See the Databricks SDK `Config` class.
        config
            A Databricks SDK `Config` to authenticate with, instead of the parameters above.
        scopes
            The OAuth scopes to request. See the Databricks SDK `Config` class.
        authorization_details
            The OAuth authorization details to request. See the Databricks SDK `Config` class.
        custom_headers
            HTTP headers added to every request sent by the Databricks SDK.
        client
            A Databricks SDK `WorkspaceClient` to call the APIs with. It is not picklable.
        operation_concurrency : int, optional
            The maximum number of concurrent API requests of a recursive operation, such as `rm` or `copy`. Each
            backend has its own default.
        io_concurrency : int, optional
            The maximum number of concurrent transfers of the files opened, shared by all of them. Each backend has
            its own default.
        transfer_concurrency : int, optional
            The maximum number of files copied or moved between two backends at a time, e.g. from Workspace files
            to a Volume. Defaults to 8.
        use_local_fs_in_workspace : bool, optional
            Whether to use the local file system, rather than the Databricks APIs, when running in a Databricks
            workspace. Defaults to True.
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
            transfer_concurrency=transfer_concurrency or self.transfer_concurrency,
            # No Databricks file system caches listings.
            use_listings_cache=False,
            **storage_options,
        )
        self._init_client(client, config, config_params)

        self._storage_options = storage_options

        self._operation_concurrency = operation_concurrency
        self._io_concurrency = io_concurrency
        if use_local_fs_in_workspace is not None:
            self.use_local_fs_in_workspace = use_local_fs_in_workspace

        self.__local_fs: DatabricksLocalFileSystem | None = None

        self._running_in_workspace: bool | None = None
        self._legacy_dbfs_enabled: bool | None = None

        _register_cleanup(self)

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)

    def close(self):
        """Close the file system and its backends."""
        try:
            if not self.closed:
                self._close_client()
        finally:
            self.__local_fs = None
            AsyncRoutingFileSystem.close(self)

    def __enter__(self):
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        with contextlib.suppress(Exception):
            self.close()

    ### The backends, created on first use
    def _api_backend(self, key: str, cls: type) -> AsyncFileSystem:
        if self.closed:
            raise RuntimeError("The file system is already closed")
        return self._backend(
            key,
            lambda: cls(
                client=self.client,
                config=self.config,
                operation_concurrency=self._operation_concurrency,
                io_concurrency=self._io_concurrency,
                **self._storage_options,
            ),
        )

    @property
    def _volume_fs(self) -> VolumeFileSystem:
        """The Volume backend, created on first use."""
        return self._api_backend("volume", VolumeFileSystem)

    @property
    def _workspace_fs(self) -> WorkspaceFileSystem:
        """The Workspace backend, created on first use."""
        return self._api_backend("workspace", WorkspaceFileSystem)

    @property
    def _legacy_fs(self) -> DBFS:
        """The legacy DBFS backend, created on first use."""
        return self._api_backend("legacy", DBFS)

    @property
    def _local_fs(self) -> DatabricksLocalFileSystem:
        """The local file system, created on first use."""
        if self.closed:
            raise RuntimeError("The file system is already closed")

        if self.__local_fs is None:
            self.__local_fs = DatabricksLocalFileSystem(**self._storage_options)
        return self.__local_fs

    @property
    def _local_tree(self) -> SyncTreeWrapper:
        """The local file system as the async backend the router needs."""
        local = self._local_fs
        return self._backend(
            "local",
            lambda: SyncTreeWrapper(
                local,
                operation_concurrency=self._operation_concurrency
                or DBFS.operation_concurrency,
                io_concurrency=self._io_concurrency or DBFS.io_concurrency,
                # FUSE mounts, e.g. of Volumes, may not rename folders: nothing is declared.
                storage_profile=StorageProfile(),
            ),
        )

    @property
    def running_in_workspace(self) -> bool:
        """Whether the code runs in a Databricks workspace."""
        if self._running_in_workspace is None:
            self._running_in_workspace = "DATABRICKS_RUNTIME_VERSION" in os.environ
        return self._running_in_workspace

    @property
    def legacy_dbfs_enabled(self) -> bool:
        """Whether legacy DBFS is enabled in the workspace.

        With the local file system, it is enabled when its local mount exists: no API is called.
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

    ### Routing, for AsyncRoutingFileSystem
    def _route(self, path: str) -> tuple[AsyncFileSystem, str] | None:
        if path.rstrip("/") == "":
            return None  # The root spans the backends: _roots lists it.

        t = fs_type(path)
        if self._uses_local_fs:
            return self._local_tree, to_local_path(path, self.legacy_dbfs_local_root)
        if t == FileSystemType.Workspace:
            return self._workspace_fs, path
        elif t == FileSystemType.Volume:
            return self._volume_fs, path
        # _prepare_route has asked, except before a sync call of an async router: then this blocks.
        elif self.legacy_dbfs_enabled:
            return self._legacy_fs, path
        raise file_not_found_error(path)

    def _outer(self, fs: AsyncFileSystem, path: str) -> str:
        if self._uses_local_fs:
            return from_local_path(path, self.legacy_dbfs_local_root)
        return path

    async def _prepare_route(self, path: str) -> None:
        # Only legacy DBFS paths need the workspace setting, asked once in a thread and then cached.
        if self._legacy_dbfs_enabled is None and fs_type(path) not in (
            FileSystemType.Workspace,
            FileSystemType.Volume,
        ):
            await asyncio.to_thread(lambda: self.legacy_dbfs_enabled)

    async def _roots(self, path: str) -> list[dict[str, Any]]:
        if not await asyncio.to_thread(lambda: self.legacy_dbfs_enabled):
            return [dict(volumes_root_info), dict(workspace_root_info)]
        # The root of legacy DBFS, which holds the mounts of the other backends.
        if self._uses_local_fs:
            fs, inner = self._local_tree, self.legacy_dbfs_local_root
        else:
            fs, inner = self._legacy_fs, "/"
        return [self._entry(fs, e) for e in await fs._ls(inner, detail=True)]

    async def _root_info(self, path: str) -> dict[str, Any]:
        # _route gives None for the root only.
        return dict(root_dir_info)

    def _transfer(self, fs1, path1, fs2, path2, *, copy_file):
        # Every backend is a TreeFileSystem; anything else gets fsspec's own copy.
        if not isinstance(fs1, TreeFileSystem):
            return None
        return tree_transfer(
            path1,
            path2,
            ls=fs1._ls,
            info=fs1._info,
            copy_file=copy_file,
            dst_info=fs2._info,
            dst_makedirs=fs2._makedirs,
            dst_rm_file=fs2._rm_file,
            profile=getattr(fs2, "storage_profile", None) or StorageProfile(),
            # Not transfer_limit: copy_file holds one of its permits.
            concurrency_limit=fs1.operation_limit,
        )

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
        """Open a file in a Unity Catalog Volume, Workspace files or legacy DBFS.

        Inside `fs.transaction`, a file written is committed or discarded with the others, whatever its backend.

        Parameters
        ----------
        path : str
            The path to the file: a `dbfs:/` URL or a POSIX path.
        mode : str, optional
            The file mode, such as `rb` or `w`. The modes supported depend on the backend.
        notebook_format : NotebookFormat, optional
            The format of a notebook read or written. Applies only to Workspace notebooks.
        language : Language, str, optional
            The language of a notebook written. Applies only to Workspace notebooks.
        compression : str, optional
            The compression codec: a key of ``fsspec.compression.compr``, or "infer" to guess it from the file
            name's suffix.
        encoding : str, optional
            The text encoding, in text mode only. See `io.TextIOWrapper
            <https://docs.python.org/3/library/io.html#io.TextIOWrapper>`_.
        errors : str, optional
            How encoding errors are handled, in text mode only. See `io.TextIOWrapper
            <https://docs.python.org/3/library/io.html#io.TextIOWrapper>`_.
        newline : str, optional
            How newlines are handled, in text mode only. The backends write "\\n" newlines by default. See
            `io.TextIOWrapper <https://docs.python.org/3/library/io.html#io.TextIOWrapper>`_.
        **kwargs
            Passed to the backend's `open`.
        """
        if notebook_format is not None:
            kwargs["notebook_format"] = notebook_format
        if language is not None:
            kwargs["language"] = language
        if "b" not in mode:
            # Text is any mode without "b": "w" as well as "wt".
            if write_mode(mode) and newline is None:
                newline = "\n"
            kwargs.update(encoding=encoding, errors=errors, newline=newline)
        return super().open(path, mode, compression=compression, **kwargs)


__all__ = [
    "DatabricksFileSystem",
]

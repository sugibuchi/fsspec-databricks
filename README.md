# fsspec-databricks

![PyPI - Version](https://img.shields.io/pypi/v/fsspec-databricks?color=blue)
[![codecov](https://codecov.io/github/sugibuchi/fsspec-databricks/graph/badge.svg?token=RKC8T20CEE)](https://codecov.io/github/sugibuchi/fsspec-databricks)

File system interface for Databricks file system"s".

`fsspec-databricks` provides a [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)-compliant file system
implementation that unifies access to Databricks file systems, including:

* [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
* [Workspace files](https://docs.databricks.com/aws/en/files/workspace)
* [Legacy DBFS (Databricks File System)](https://docs.databricks.com/aws/en/dbfs/)

The library routes `dbfs:/` and POSIX-style paths to the appropriate Databricks file system implementation
and supports copying and streaming between them.

## Features

* Provides seamless access to files in different Databricks file systems with DBFS URLs (`dbfs:/path/to/file`) or POSIX
  paths (`/path/to/file`).
    * Automatically routes file operations to appropriate file systems based on file path patterns.
    * Implements file operations across different file systems, for example, copying a file from Workspace to Unity
      Catalog Volume or vice versa.
* Fallbacks to the local file system access when running inside a Databricks workspace.
* Implemented on [Databricks Python SDK](https://github.com/databricks/databricks-sdk-py).
    * Uses [Databricks Unified Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth)

## Compatibility

* Python 3.10 to 3.14
* `databricks-sdk`: 0.99.0 or later
* Databricks workspace: Tested on the following environments at the moment.
    * Azure Databricks
    * Databricks Free Edition

## Project status

The current status of this library is **early beta**. Its API and behavior are subject to change during further
development and testing.

* The current version relies on the undocumented multipart upload API for Unity
  Catalog Volume file write, which Databricks does not officially support and may change without notice.
* For more details about the current limitations, see the [Limitations](#limitations) section below.

## Getting started

### Installation

You can install [`fsspec-databricks`](https://pypi.org/project/fsspec-databricks/) from PyPI.

```bash
# with pip
pip install fsspec-databricks
# with UV
uv add fsspec-databricks
```

### Usage

Then you can directly instantiate `DatabricksFileSystem` in `fsspec_databricks` module.

```python
from fsspec_databricks import DatabricksFileSystem

fs = DatabricksFileSystem()
```

Or, you can register `DatabricksFileSystem` as the default file system implementation for `dbfs:/` URL scheme
by calling `fsspec_databricks.use()`.

```python
import fsspec
import fsspec_databricks

fsspec_databricks.use()

fs = fsspec.filesystem("dbfs")  # DatabricksFileSystem
```

For more details on how to use the fsspec file system objects,
see [fsspec's documentation](https://filesystem-spec.readthedocs.io/en/latest/usage.html).

### Supported file paths

`fsspec-databricks` supports file paths with `dbfs:/` scheme.

It uses the same path patterns as Databricks to map `dbfs:/` and POSIX paths to the appropriate file system
implementation.

| URL pattern                                              | Mapped file system               |
|----------------------------------------------------------|----------------------------------|
| `dbfs:/Volumes/(catalog)/(schema)/(volume)/path/to/file` | Unity Catalog Volume file system |
| `dbfs:/Workspace/path/to/file`                           | Databricks Workspace file system |
| `dbfs:/...` (other than above)                           | Legacy DBFS (deprecated)         |

Examples:

```python
fs.ls("dbfs:/Volumes/my_catalog/my_schema/my_volume/path")  # Access Unity Catalog Volume files
fs.ls("dbfs:/Workspace/Users/user-a/path")  # Access workspace files
fs.ls("dbfs:/data/path")  # Access legacy DBFS files
```

`fsspec-databricks` supports also stripped, POSIX-like paths without `dbfs:/` scheme.

| Path pattern                                        | Mapped file system                                                 |
|-----------------------------------------------------|--------------------------------------------------------------------|
| `/Volumes/(catalog)/(schema)/(volume)/path/to/file` | Unity Catalog Volume file system                                   |
| `/Workspace/path/to/file`                           | Databricks Workspace file system (only in DBFS-disabled workspace) |
| `/...` (other than above)                           | Legacy DBFS (deprecated)                                           |

Examples:

```python
fs.ls("/Volumes/my_catalog/my_schema/my_volume/path")  # Access Unity Catalog Volume files
fs.ls("/Workspace/Users/user-a/path")  # Access workspace files (only in DBFS-disabled workspace)
fs.ls("/data/path")  # Access legacy DBFS files
```

For more details about`dbfs:/` and POSIX path support in Databricks, see
[the official documentation](https://docs.databricks.com/aws/en/files/).

## Authentication

`fsspec-databricks` uses Databricks Unified Authentication provided by Databricks Python SDK.

You can find information about supported authentication parameters and environment variables in
the [Databricks Python SDK documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html).

### Default authentication

If Databricks Unified Authentication is configured, `fsspec-databricks` will pick up credentials from the default
profile. For more, see the above Databricks SDK docs.

```python
from fsspec_databricks.spec import DatabricksFileSystem

fs = DatabricksFileSystem()

with fs.open("dbfs:/Volumes/...") as f:
    ...
```

### Via constructor parameters

You can programmatically configure authentication by passing parameters to `DatabricksFileSystem` constructor.

```python
# Authentication with PAT
fs = DatabricksFileSystem(host=host_url, token=access_token)

# Use different profile
fs = DatabricksFileSystem(profile="production")
```

### Via environment variables

Or, you can configure authentication via environment variables.

```bash
# Shell
export DATABRICKS_CONFIG_PROFILE=production
```

```python
# Then in Python
fs = DatabricksFileSystem()  # will use the "production" profile
```

### By `fsspec` configuration

You can use
the [fsspec's configuration model](https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration) to
configure and persist authentication parameters.

### With `WorkspaceClient`

You can create `DatabricksFileSystem` by explicitly setting Databricks SDK's `WorkspaceClient` object.
The created `DatabricksFileSystem` instance will use the authentication configured in the provided `WorkspaceClient`
object.

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient(...)
...

fs = DatabricksFileSystem(client=client)
```

Note: a `DatabricksFileSystem` created with a `WorkspaceClient` will generally not be serializable, because
`WorkspaceClient` instances are not serializable. Consider using other configuration methods for if you need
serializable filesystem objects.

## Configuration options

In addition to the authentication parameters, `fsspec-databricks` supports the following configuration options.

### Options for general file system behavior

| Parameter name            | Description                                                                                                                                  | Default |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|---------|
| config                    | An optional pre-configured Databricks SDK `Config` object. If provided, it will be used for authentication.                                  | `None`  |
| client                    | An optional pre-configured Databricks SDK `WorkspaceClient` object. If provided, it will be used for accessing the Databricks Workspace API. | `None`  |
| use_local_fs_in_workspace | Access files from the local file system rather than the remote Databricks API when running within a Databricks workspace.                    | `True`  |
| verbose_debug_log         | Whether to enable verbose debug logging for file system operations.                                                                          | `False` |

### Options for Unity Catalog Volume file system

| Parameter name                  | Description                                                                             | Default                    |
|---------------------------------|-----------------------------------------------------------------------------------------|----------------------------|
| volume_fs_max_read_concurrency  | The maximum number of concurrent file read operations on a Unity Catalog Volume file.   | `10`                       |
| volume_fs_min_read_block_size   | The minimum data size to read for each read operation on a Unity Catalog Volume file.   | `512 * 1024` (512 kb)      |
| volume_fs_max_read_block_size   | The maximum data size to read for each read operation on a Unity Catalog Volume file.   | `8 * 1024 * 1024` (8 mb)   |
| volume_fs_max_write_concurrency | The maximum number of concurrent file write operations on a Unity Catalog Volume file.  | `10`                       |
| volume_fs_min_write_block_size  | The minimum data size to write for each write operation on a Unity Catalog Volume file. | `5 * 1024 * 1024` (5 mb)   |
| volume_fs_max_write_block_size  | The maximum data size to write for each write operation on a Unity Catalog Volume file. | `32 * 1024 * 1024` (32 mb) |

## Differences from the original `DatabricksFileSystem` in `fsspec`

`fsspec` provides its own implementation of `DatabricksFileSystem` (`fsspec.implementations.DatabricksFileSystem`).

The main difference between `DatabricksFileSystem` in `fsspec-databricks` and the original one in `fsspec` is that
the original one is for [legacy DBFS (Databricks File System)](https://docs.databricks.com/aws/en/dbfs/),
which Databricks has already deprecated.

Databricks currently supports workspace files and Unity Catalog volumes in addition to the legacy DBFS,
and it continues to use the `dbfs:/` URL scheme for both legacy DBFS and the other file systems
([documentation](https://docs.databricks.com/aws/en/files/)).

`fsspec-databricks` primarily aims to support new file systems (workspace files and Unity Catalog volumes)
and enable seamless access to them using the same `dbfs:/` URL scheme supported in Databricks workspaces.

## Limitations

The following features are not yet implemented or have not been tested yet.

* Compatibility with Databricks on AWS and Databricks on GCP (not tested)
* Resumable file upload for Unity Catalog Volume files (not implemented, required for Databricks on GCP)
* Legacy DBFS support (not tested)
* Use of the storage proxy when running inside a Databricks workspace or notebook (not implemented)

We are actively developing and testing the library, and we welcome contributions and feedback from the community.

## License

Apache License 2.0. See [LICENSE](LICENSE) for more details.

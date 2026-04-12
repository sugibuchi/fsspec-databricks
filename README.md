# fsspec-databricks

[![PyPI - Version](https://img.shields.io/pypi/v/fsspec-databricks?color=blue)](https://pypi.org/project/fsspec-databricks/)
[![codecov](https://codecov.io/github/sugibuchi/fsspec-databricks/graph/badge.svg?token=RKC8T20CEE)](https://codecov.io/github/sugibuchi/fsspec-databricks)

File system interface for Databricks file system"s".

`fsspec-databricks` provides a [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)-compliant file system
implementation that unifies access to Databricks file systems, including:

* [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
* [Workspace files](https://docs.databricks.com/aws/en/files/workspace)
* [Legacy DBFS (Databricks File System)](https://docs.databricks.com/aws/en/dbfs/)

The library routes `dbfs:/` and POSIX-style paths to the appropriate Databricks file system implementation
and supports copying and streaming between them.

---

## Features

* Provides seamless access to files in different Databricks file systems with DBFS URLs (`dbfs:/path/to/file`) or POSIX
  paths (`/path/to/file`).
    * Automatically routes file operations to appropriate file systems based on file path patterns.
    * Implements file operations across different file systems, for example, copying a file from Workspace to Unity
      Catalog Volume or vice versa.
* Falls back to the local file system when running inside a Databricks workspace.
* Implemented on [Databricks Python SDK](https://github.com/databricks/databricks-sdk-py).
    * Uses [Databricks Unified Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth)

---

## Compatibility

* Python 3.10 to 3.14
* `databricks-sdk`: 0.99.0 or later
* `fsspec`: 2024.6.0 or later
* `aiohttp`: 3.12.0 or later
* Databricks workspace: Tested on the following environments at the moment.
    * Databricks Free Edition
    * Azure Databricks
    * Databricks on Google Cloud

---

## Project status

The current status of this library is **early beta**. Its API and behavior are subject to change during further
development and testing.

* The current version relies on the undocumented multipart upload API for Unity
  Catalog Volume file write, which Databricks does not officially support and may change without notice.
* For more details about the current limitations, see the [Limitations](#limitations) section below.

---

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

For detailed usage — common operations, POSIX paths, backend-specific behaviour and configuration parameters,
see [USAGE.md](USAGE.md).

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

For more details about `dbfs:/` and POSIX path support in Databricks, see
[the official documentation](https://docs.databricks.com/aws/en/files/).

---

## Authentication

`fsspec-databricks` uses Databricks Unified Authentication provided by Databricks Python SDK.

```python
from fsspec_databricks import DatabricksFileSystem

# Uses Databricks Unified Authentication with the default profile
fs = DatabricksFileSystem()

with fs.open("dbfs:/Volumes/...") as f:
    ...
```

For all authentication options — constructor parameters, environment variables, `fsspec` configuration,
and `WorkspaceClient` — see [AUTHENTICATION.md](AUTHENTICATION.md).

---

## Motivation

Python code running outside a Databricks cluster — on a local machine, a CI server, or a Kubernetes pod — has no
standard way to access Unity Catalog Volumes or Workspace files. The Databricks SDK provides a file API, but it is not
integrated with the Python data ecosystem: tools like pandas, PyArrow, and DuckDB cannot use it without custom glue
code.

Inside a Databricks workspace, the experience is quite different. All file systems are mounted locally via FUSE, so code
can read and write files using ordinary POSIX paths with no special handling.
[Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/index) extends this
transparency further — remote code submits Spark operations to Databricks compute as if running inside the workspace,
with no code changes required.

File access from Python, however, is not covered. FUSE mounts exist only on the cluster, and Databricks Connect does not
bridge them. Even when Spark compute is fully transparent, direct Python file I/O remains a special case that requires
different code depending on where the application runs.

`fsspec-databricks` restores that transparency for file access. It implements
the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) interface — the de-facto file system abstraction in the
Python data ecosystem — on top of the Databricks REST API, so the same application code works from any environment
without modification, using a single `dbfs:/` path that consistently identifies a file regardless of where the code
runs.

---

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

---

## Limitations

The following features are not yet implemented or have not been tested yet.

* Compatibility with Databricks on AWS (not tested)
* Legacy DBFS support (not tested)

We are actively developing and testing the library, and we welcome contributions and feedback from the community.

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for more details.

# fsspec-databricks

[![PyPI - Version](https://img.shields.io/pypi/v/fsspec-databricks?color=blue)](https://pypi.org/project/fsspec-databricks/)
[![Python versions](https://img.shields.io/pypi/pyversions/fsspec-databricks)](https://pypi.org/project/fsspec-databricks/)
[![License](https://img.shields.io/github/license/sugibuchi/fsspec-databricks)](https://github.com/sugibuchi/fsspec-databricks/blob/main/LICENSE)
[![CI](https://github.com/sugibuchi/fsspec-databricks/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/sugibuchi/fsspec-databricks/actions/workflows/ci.yaml)
[![CodeQL](https://github.com/sugibuchi/fsspec-databricks/actions/workflows/codeql.yaml/badge.svg?branch=main)](https://github.com/sugibuchi/fsspec-databricks/actions/workflows/codeql.yaml)
[![codecov](https://codecov.io/github/sugibuchi/fsspec-databricks/graph/badge.svg?token=RKC8T20CEE)](https://codecov.io/github/sugibuchi/fsspec-databricks)

An [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) file system for Databricks:

* [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
* [Workspace files](https://docs.databricks.com/aws/en/files/workspace)
* [Legacy DBFS](https://docs.databricks.com/aws/en/dbfs/)

The same `dbfs:/` URLs and POSIX paths work inside and outside Databricks, with pandas, PyArrow and any library that
takes fsspec.

---

## Features

* Routes `dbfs:/` URLs and POSIX paths to the right file system, as Databricks does.
* Copies and moves files between file systems, e.g. from Workspace files to a Volume.
* Uses the local file system when running inside a Databricks workspace.
* Reads and writes Volume files with concurrent requests.
* Authenticates with [Databricks Unified Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth).

fsspec's own `DatabricksFileSystem` supports legacy DBFS only.

---

## Installation

```bash
pip install fsspec-databricks
# or
uv add fsspec-databricks
```

---

## Usage

```python
from fsspec_databricks import DatabricksFileSystem

fs = DatabricksFileSystem()

fs.ls("dbfs:/Volumes/my_catalog/my_schema/my_volume/path")

with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data.csv", "rb") as f:
    data = f.read()
```

To use it for every `dbfs:/` URL in fsspec and the libraries using it, call `use()` once:

```python
import fsspec
import fsspec_databricks

fsspec_databricks.use()

fs = fsspec.filesystem("dbfs")  # DatabricksFileSystem
```

See [USAGE.md](https://github.com/sugibuchi/fsspec-databricks/blob/main/USAGE.md) for operations, each file system's
behaviour and options. See [AUTHENTICATION.md](https://github.com/sugibuchi/fsspec-databricks/blob/main/AUTHENTICATION.md)
for authentication.

### Paths

| Path                                                   | File system                                 |
|--------------------------------------------------------|---------------------------------------------|
| `dbfs:/Volumes/<catalog>/<schema>/<volume>/path`       | Unity Catalog Volume                        |
| `dbfs:/Workspace/path`                                 | Workspace files                             |
| `dbfs:/path`, any other                                | Legacy DBFS                                 |

POSIX paths without `dbfs:` work the same: `/Volumes/...`, `/Workspace/...` and `/path`. See
[Databricks' documentation](https://docs.databricks.com/aws/en/files/) for these paths.

---

## Compatibility

* Python 3.11 to 3.14
* `databricks-sdk`: 0.80.0 or later. Older versions ignore some parameters: `custom_headers` needs 0.86.0, and
  `group_id` needs 0.134.0.
* `fsspec`: 2024.6.0 or later
* `aiohttp`: 3.12.0 or later
* Tested live on Databricks Free Edition on AWS, Azure Databricks and Databricks on Google Cloud

---

## Status

**Early beta.** The API and behaviour may change.

* Writing Volume files uses the Files API's multipart upload. The Databricks REST reference does not document it, but
  the official Databricks SDK for Python uses it, as an experimental feature.
* Legacy DBFS is tested offline only, on a simulated API: no test workspace enables it.

---

## Development, security and license

* [DEVELOPMENT.md](https://github.com/sugibuchi/fsspec-databricks/blob/main/DEVELOPMENT.md) explains how to work on
  the package: its design, setup, tests and releases.
* [SECURITY.md](https://github.com/sugibuchi/fsspec-databricks/blob/main/SECURITY.md) explains how to report a
  vulnerability.
* The package is licensed under the Apache License 2.0. See
  [LICENSE](https://github.com/sugibuchi/fsspec-databricks/blob/main/LICENSE).

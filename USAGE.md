# Usage guide

This document covers how to use `fsspec-databricks` in detail.

For installation and authentication, see [README.md](README.md).

---

## Creating a file system instance

```python
from fsspec_databricks import DatabricksFileSystem

fs = DatabricksFileSystem()
```

You can also register `DatabricksFileSystem` as the default implementation for the `dbfs` protocol
and use it via `fsspec.filesystem()` or directly in libraries that accept fsspec URLs:

```python
import fsspec
import fsspec_databricks

fsspec_databricks.use()

fs = fsspec.filesystem("dbfs")
```

---

## Common operations

All standard fsspec operations are supported.

```python
# List a directory
fs.ls("dbfs:/Volumes/my_catalog/my_schema/my_volume/data")

# Check existence
fs.exists("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.parquet")

# Get metadata
info = fs.info("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.parquet")
print(info["size"], info["modified"])

# Make directories
fs.makedirs("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/subdir", exist_ok=True)

# Copy a file
fs.copy(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/src.csv",
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/dst.csv",
)

# Move / rename a file
fs.mv(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/old.csv",
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/new.csv",
)

# Delete a file
fs.rm("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.csv")

# Delete a directory recursively
fs.rm("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/subdir", recursive=True)

# Glob
files = fs.glob("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/*.parquet")
```

### Reading and writing files

```python
# Read entire file
with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.bin", "rb") as f:
    data = f.read()

# Write a file
with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/output.bin", "wb") as f:
    f.write(b"hello")

# Use with pandas
import pandas as pd

df = pd.read_parquet(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/data/table.parquet",
    filesystem=fs,
)
df.to_parquet(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/data/out.parquet",
    filesystem=fs,
)
```

### Using POSIX paths

`fsspec-databricks` also accepts POSIX-style paths without the `dbfs:/` prefix.
These use the same routing rules:

| POSIX path                                          | Mapped file system                                                 |
|-----------------------------------------------------|--------------------------------------------------------------------|
| `/Volumes/(catalog)/(schema)/(volume)/path/to/file` | Unity Catalog Volume file system                                   |
| `/Workspace/path/to/file`                           | Databricks Workspace file system (only in DBFS-disabled workspace) |
| `/...` (other than above)                           | Legacy DBFS (deprecated)                                           |

```python
# These are equivalent to the dbfs:/ examples above
fs.ls("/Volumes/my_catalog/my_schema/my_volume/data")

with fs.open("/Volumes/my_catalog/my_schema/my_volume/data/file.bin", "rb") as f:
    data = f.read()

with fs.open("/Workspace/Users/alice@example.com/output.txt", "wb") as f:
    f.write(b"hello")
```

---

## Local file system fallback

When code runs inside a Databricks cluster or notebook, `DatabricksFileSystem` can transparently
redirect all file operations to the local file system instead of making remote API calls.
This avoids unnecessary network overhead because Databricks mounts Unity Catalog Volumes and
Workspace files locally.

This behaviour is **enabled by default** (`use_local_fs_in_workspace=True`). It activates
automatically when the `DATABRICKS_RUNTIME_VERSION` environment variable is set, which Databricks
sets on all clusters.

```python
# Default: uses local FS when running on a Databricks cluster
fs = DatabricksFileSystem()

# Opt out: always use the remote API, even on a cluster
fs = DatabricksFileSystem(use_local_fs_in_workspace=False)
```

When the local fallback is active, **all** paths — Volumes, Workspace, and DBFS — are served
from the local file system. The `dbfs:/` scheme and POSIX paths are translated to their local
mount points automatically.

---

## Cross-backend operations

`DatabricksFileSystem` supports copying and moving files between different backends transparently.

```python
# Copy from Workspace to a Volume
fs.copy(
    "dbfs:/Workspace/Users/alice@example.com/data.csv",
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/data.csv",
)

# Move from a Volume to Workspace
fs.mv(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/output.txt",
    "dbfs:/Workspace/Users/alice@example.com/output.txt",
)
```

---

## Unity Catalog Volume files

Paths that match `/Volumes/(catalog)/(schema)/(volume)/...` are handled by the Unity Catalog
Volume backend.

```python
fs.ls("dbfs:/Volumes/my_catalog/my_schema/my_volume/data")

with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.parquet", "rb") as f:
    data = f.read()

with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/output.bin", "wb") as f:
    f.write(b"hello")
```

### Async I/O and concurrency

The Unity Catalog Volume backend uses asynchronous HTTP (`aiohttp`) with a dedicated background
event loop to achieve high-throughput concurrent reads and writes.

**Reads** issue multiple byte-range requests in parallel. The block size adapts upward
automatically as more consecutive bytes are read, so sequential scans benefit from larger blocks
over time.

**Writes** buffer data in memory and switch to multipart upload once the buffer exceeds
`volume_min_multipart_upload_size` (default 5 MB). Parts are uploaded concurrently. If the server
does not support multipart upload, the backend falls back to a resumable (single-stream) upload
automatically.

### Storage proxy routing

When running inside a Databricks workspace, the backend probes for an internal storage proxy on
first use. If the proxy is reachable, all I/O is routed through it directly. Otherwise the backend
falls back to presigned URLs issued by the Databricks REST API.

### Configuration parameters

These parameters can be passed to `DatabricksFileSystem` (prefixed with `volume_fs_`) or directly
to `VolumeFileSystem`:

| `DatabricksFileSystem` parameter   | `VolumeFileSystem` parameter | Description                                                                          | Default |
|------------------------------------|------------------------------|--------------------------------------------------------------------------------------|---------|
| `volume_fs_max_read_concurrency`   | `max_read_concurrency`       | Maximum number of concurrent range requests when reading a file.                     | `24`    |
| `volume_fs_min_read_block_size`    | `min_read_block_size`        | Minimum byte-range size per read request.                                            | `1 MB`  |
| `volume_fs_max_read_block_size`    | `max_read_block_size`        | Maximum byte-range size per read request.                                            | `4 MB`  |
| `volume_fs_max_write_concurrency`  | `max_write_concurrency`      | Maximum number of concurrent part uploads when writing a file.                       | `24`    |
| `volume_fs_min_write_block_size`   | `min_write_block_size`       | Minimum part size for multipart upload.                                              | `5 MB`  |
| `volume_fs_max_write_block_size`   | `max_write_block_size`       | Maximum part size for multipart upload.                                              | `16 MB` |
| `volume_min_multipart_upload_size` | `min_multipart_upload_size`  | File size threshold above which multipart upload is used instead of a single PUT.    | `5 MB`  |
| `volume_fs_connection_pool_size`   | `connection_pool_size`       | Maximum number of connections in the aiohttp connection pool (`TCPConnector` limit). | `100`   |

Example — tuning for large sequential reads:

```python
fs = DatabricksFileSystem(
    volume_fs_max_read_concurrency=32,
    volume_fs_max_read_block_size=8 * 1024 * 1024,  # 8 MB blocks
)
```

### Supported modes

| Mode  | Supported | Notes                            |
|-------|-----------|----------------------------------|
| `rb`  | Yes       |                                  |
| `wb`  | Yes       |                                  |
| `xb`  | Yes       | Fails if the file already exists |
| `ab`  | No        | Append mode is not supported     |
| `r+b` | No        | Update mode is not supported     |

---

## Workspace files

Paths that match `/Workspace/...` are handled by the Workspace backend via the Databricks
Workspace API.

```python
fs.ls("dbfs:/Workspace/Users/alice@example.com/")

with fs.open("dbfs:/Workspace/Users/alice@example.com/data.csv", "rb") as f:
    data = f.read()

with fs.open("dbfs:/Workspace/Users/alice@example.com/output.txt", "wb") as f:
    f.write(b"hello")
```

> **Note:** The maximum file size for Workspace files is 10 MB, which is a Databricks API limit.

### Notebook access

The Workspace backend can read and write Databricks notebook objects in addition to regular files.
Notebooks are identified automatically by the Workspace API.

Use the `notebook_format` parameter to control the export/import format:

| `notebook_format` value | Description                            |
|-------------------------|----------------------------------------|
| `"SOURCE"` (default)    | Source code in the notebook's language |
| `"JUPYTER"`             | Jupyter Notebook (`.ipynb`) format     |
| `"HTML"`                | HTML export (read-only)                |
| `"DBC"`                 | Databricks archive format              |
| `"R_MARKDOWN"`          | R Markdown format (R notebooks only)   |

```python
# Read a notebook as source code (default)
with fs.open("dbfs:/Workspace/Users/alice@example.com/my_notebook", "rb") as f:
    source = f.read()

# Read a notebook as a Jupyter notebook (.ipynb)
with fs.open(
        "dbfs:/Workspace/Users/alice@example.com/my_notebook",
        "rb",
        notebook_format="JUPYTER",
) as f:
    ipynb = f.read()

# Write a new Python notebook
with fs.open(
        "dbfs:/Workspace/Users/alice@example.com/new_notebook",
        "wb",
        notebook_format="SOURCE",
        language="PYTHON",
) as f:
    f.write(b"display('hello')")
```

When writing a **new** notebook, `language` is required and must be one of `"PYTHON"`, `"SCALA"`,
`"SQL"`, or `"R"`. When overwriting an existing notebook, the language is taken from the existing
object and `language` can be omitted.

### Supported modes

| Mode  | Supported | Notes                                               |
|-------|-----------|-----------------------------------------------------|
| `rb`  | Yes       |                                                     |
| `wb`  | Yes       |                                                     |
| `ab`  | Yes       | Existing content is downloaded first, then appended |
| `xb`  | Yes       | Fails if the file already exists                    |
| `r+b` | No        | Update mode is not supported                        |

---

## Legacy DBFS files

Paths that do not match `/Volumes/...` or `/Workspace/...` are routed to the legacy DBFS backend.

```python
fs.ls("dbfs:/mnt/my-mount")

with fs.open("dbfs:/mnt/my-mount/data.csv", "rb") as f:
    data = f.read()
```

> **Note:** Databricks has deprecated legacy DBFS. Prefer Unity Catalog Volumes for new workloads.
> Legacy DBFS compatibility has not been fully tested in `fsspec-databricks`.

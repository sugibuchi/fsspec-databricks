# Usage

For installation, see [README.md](README.md). For authentication, see [AUTHENTICATION.md](AUTHENTICATION.md).

---

## Creating a file system

```python
from fsspec_databricks import DatabricksFileSystem

fs = DatabricksFileSystem()
```

Or register it for the `dbfs` protocol, for `fsspec.filesystem()` and libraries taking fsspec URLs:

```python
import fsspec
import fsspec_databricks

fsspec_databricks.use()

fs = fsspec.filesystem("dbfs")
```

### Options

Besides the [authentication parameters](AUTHENTICATION.md):

| Parameter                   | Description                                                                         | Default |
|-----------------------------|-------------------------------------------------------------------------------------|---------|
| `config`                    | A Databricks SDK `Config` to authenticate with.                                     | `None`  |
| `client`                    | A Databricks SDK `WorkspaceClient` to call the APIs with.                           | `None`  |
| `use_local_fs_in_workspace` | Use the local file system when running in a Databricks workspace.                   | `True`  |
| `verbose_debug_log`         | Log every operation at debug level.                                                 | `False` |

Unity Catalog Volumes have [options of their own](#options-for-volumes).

---

## Common operations

All standard fsspec operations are supported.

```python
# List a folder
fs.ls("dbfs:/Volumes/my_catalog/my_schema/my_volume/data")

# Check existence
fs.exists("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.parquet")

# Get metadata
info = fs.info("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.parquet")
print(info["size"], info["modified"])

# Create folders
fs.makedirs("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/subdir", exist_ok=True)

# Copy a file
fs.copy(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/src.csv",
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/dst.csv",
)

# Move or rename a file
fs.mv(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/old.csv",
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/new.csv",
)

# Delete a file
fs.rm("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.csv")

# Delete a folder
fs.rm("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/subdir", recursive=True)

# Glob
files = fs.glob("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/*.parquet")
```

Errors are standard exceptions: `FileNotFoundError`, `FileExistsError`, `IsADirectoryError`, `NotADirectoryError`,
`PermissionError`, or `OSError`.

### Reading and writing files

```python
# Read a file
with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/file.bin", "rb") as f:
    data = f.read()

# Write a file
with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/output.bin", "wb") as f:
    f.write(b"hello")

# With pandas
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

### POSIX paths

Paths without `dbfs:` work the same:

```python
fs.ls("/Volumes/my_catalog/my_schema/my_volume/data")

with fs.open("/Workspace/Users/alice@example.com/output.txt", "wb") as f:
    f.write(b"hello")
```

### Between file systems

Copies and moves work between Volumes, Workspace files and legacy DBFS:

```python
# From Workspace files to a Volume
fs.copy(
    "dbfs:/Workspace/Users/alice@example.com/data.csv",
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/data.csv",
)

# From a Volume to Workspace files
fs.mv(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/output.txt",
    "dbfs:/Workspace/Users/alice@example.com/output.txt",
)
```

---

## Inside a Databricks workspace

On a Databricks cluster or notebook, file operations use the local file system by default. `/Volumes/...` and
`/Workspace/...` are the local mounts Databricks provides.

To use the Databricks APIs there too:

```python
fs = DatabricksFileSystem(use_local_fs_in_workspace=False)
```

---

## Unity Catalog Volumes

Paths of the form `/Volumes/<catalog>/<schema>/<volume>/...`.

```python
fs.ls("dbfs:/Volumes/my_catalog/my_schema/my_volume/data")
```

`fs.ls("dbfs:/Volumes")` lists the catalogs, then the schemas and volumes below.

Reads and writes run concurrent requests. Large writes use multipart upload, or resumable upload where the workspace
requires it.

### Modes

| Mode  | Supported | Notes                            |
|-------|-----------|----------------------------------|
| `rb`  | Yes       |                                  |
| `wb`  | Yes       |                                  |
| `xb`  | Yes       | Fails if the file already exists |
| `ab`  | No        |                                  |
| `r+b` | No        |                                  |

### Options for Volumes

These set the defaults of every file opened. Pass them to `DatabricksFileSystem` with the `volume_fs_` prefix, or to
`VolumeFileSystem` without it:

| `DatabricksFileSystem` parameter   | `VolumeFileSystem` parameter | Description                                               | Default |
|------------------------------------|------------------------------|-----------------------------------------------------------|---------|
| `volume_fs_max_read_concurrency`   | `max_read_concurrency`       | Maximum concurrent requests when reading a file.          | `24`    |
| `volume_fs_min_read_block_size`    | `min_read_block_size`        | Minimum bytes per read request.                           | `1 MB`  |
| `volume_fs_max_read_block_size`    | `max_read_block_size`        | Maximum bytes per read request.                           | `4 MB`  |
| `volume_fs_max_write_concurrency`  | `max_write_concurrency`      | Maximum concurrent part uploads when writing a file.      | `24`    |
| `volume_fs_min_write_block_size`   | `min_write_block_size`       | Minimum part size.                                        | `5 MB`  |
| `volume_fs_max_write_block_size`   | `max_write_block_size`       | Maximum part size.                                        | `16 MB` |
| `volume_min_multipart_upload_size` | `min_multipart_upload_size`  | Files smaller than this are uploaded in one request.      | `5 MB`  |
| `volume_fs_connection_pool_size`   | `connection_pool_size`       | Maximum HTTP connections.                                 | `100`   |

For a slow or distant network, lower the concurrency:

```python
fs = DatabricksFileSystem(
    volume_fs_max_read_concurrency=4,
    volume_fs_max_write_concurrency=4,
    volume_fs_connection_pool_size=10,
)
```

### Options per file

`fs.open()` takes these for one file:

| Parameter         | Applies to  | Description                                                                         |
|-------------------|-------------|-------------------------------------------------------------------------------------|
| `max_concurrency` | read, write | Maximum concurrent requests.                                                        |
| `min_block_size`  | read, write | Minimum block size. A multiple of 256 KB for writes.                                |
| `max_block_size`  | read, write | Maximum block size. A multiple of 256 KB for writes.                                |
| `block_size`      | read, write | A fixed block size, setting both the minimum and the maximum.                       |

```python
with fs.open(
    "dbfs:/Volumes/my_catalog/my_schema/my_volume/data/large.parquet",
    "rb",
    max_concurrency=32,
    max_block_size=16 * 1024 * 1024,  # 16 MB blocks
) as f:
    data = f.read()
```

---

## Workspace files

Paths of the form `/Workspace/...`.

```python
fs.ls("dbfs:/Workspace/Users/alice@example.com/")

with fs.open("dbfs:/Workspace/Users/alice@example.com/data.csv", "rb") as f:
    data = f.read()
```

Files are at most 10 MB, the Workspace API's limit.

### Notebooks

Notebooks are read and written like files. `notebook_format` sets the format:

| `notebook_format`    | Format                                 |
|----------------------|----------------------------------------|
| `"SOURCE"` (default) | Source code in the notebook's language |
| `"JUPYTER"`          | Jupyter notebook (`.ipynb`)            |
| `"HTML"`             | HTML, read only                        |
| `"DBC"`              | Databricks archive                     |
| `"R_MARKDOWN"`       | R Markdown, R notebooks only           |

```python
# Read a notebook as source code
with fs.open("dbfs:/Workspace/Users/alice@example.com/my_notebook", "rb") as f:
    source = f.read()

# Read a notebook as .ipynb
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

A new notebook needs `language`: `"PYTHON"`, `"SCALA"`, `"SQL"` or `"R"`. An existing notebook keeps its language.

### Modes

| Mode  | Supported | Notes                                     |
|-------|-----------|-------------------------------------------|
| `rb`  | Yes       |                                           |
| `wb`  | Yes       |                                           |
| `ab`  | Yes       |                                           |
| `xb`  | Yes       | Fails if the file already exists          |
| `r+b` | No        |                                           |

---

## Legacy DBFS

Paths other than `/Volumes/...` and `/Workspace/...`.

```python
fs.ls("dbfs:/mnt/my-mount")

with fs.open("dbfs:/mnt/my-mount/data.csv", "rb") as f:
    data = f.read()
```

Databricks has deprecated legacy DBFS. Legacy DBFS support is not tested.

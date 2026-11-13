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

---

## Options

Besides the [authentication parameters](AUTHENTICATION.md):

| Parameter                   | Description                                                                       | Default     |
|-----------------------------|-----------------------------------------------------------------------------------|-------------|
| `config`                    | A Databricks SDK `Config` to authenticate with.                                   | `None`      |
| `client`                    | A Databricks SDK `WorkspaceClient` to call the APIs with.                         | `None`      |
| `use_local_fs_in_workspace` | Use the local file system when running in a Databricks workspace.                 | `True`      |
| `operation_concurrency`     | Maximum concurrent API requests of a recursive operation, such as `rm` or `copy`. | Per backend |
| `io_concurrency`            | Maximum concurrent transfers of the files opened, shared by all of them.          | Per backend |
| `transfer_concurrency`      | Maximum files copied or moved between two backends at a time.                     | `8`         |

Unity Catalog Volumes have [defaults of their own](#options-for-volumes). Workspace files and legacy DBFS default to
`16` and `8`. Workspace files are transferred whole, so their `io_concurrency` limits only the methods taking many paths,
such as `cat` and `put`.

Recursive operations, such as `find`, `glob`, `du`, `rm` and `copy`, run concurrent requests within one backend.

---

## Logging

The library logs under the `fsspec_databricks` logger, and installs no handlers. To trace each request to Databricks,
enable its debug level:

```python
import logging

logging.getLogger("fsspec_databricks").setLevel(logging.DEBUG)
```

Retries and aborted uploads are logged as warnings.

Operations, such as `find`, `rm` or `copy`, are reported by the monitors of `fsspec_alt_strategies`, a dependency.
For example, `LoggingMonitor` logs the operations run in a `monitoring` block:

```python
import logging

from fsspec_alt_strategies import LoggingMonitor, monitoring

logger = logging.getLogger("my_app")

with monitoring(LoggingMonitor(logger, level=logging.INFO)):
    fs.rm("dbfs:/Volumes/catalog/schema/volume/tmp", recursive=True)
```

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

### Transactions

Files written in `with fs.transaction:` become visible when the block ends. If the block raises, they are discarded:

```python
with fs.transaction:
    with fs.open("dbfs:/Volumes/my_catalog/my_schema/my_volume/data/a.bin", "wb") as f:
        f.write(b"a")
    with fs.open("dbfs:/Workspace/Users/alice@example.com/b.bin", "wb") as f:
        f.write(b"b")
```

Transactions work on `DatabricksFileSystem`, across backends, and on each backend's own file system. Each backend
keeps a file invisible in its own way:

| Backend         | Before commit                                  | On commit                    | On discard            |
|-----------------|------------------------------------------------|------------------------------|-----------------------|
| Volumes         | The parts are uploaded, but the last is held   | The upload is completed      | The upload is aborted |
| Workspace files | The file is held in memory                     | The file is uploaded         | Nothing is uploaded   |
| Legacy DBFS     | The file is written to a hidden temporary path | The file is moved into place | The file is deleted   |

Limits:

- The files are committed one after another. A transaction is not atomic across files or backends.
- Only files written with `open`, `pipe_file` or `put_file` join the transaction. Copies, moves and deletes take
  effect at once.
- Files opened with `open_async` take no part in the transaction.
- On legacy DBFS, other clients may see the temporary file. Commit deletes the file it replaces, then moves the new one:
  two requests.
- Exclusive creation, mode `"x"`, is checked when opening. Legacy DBFS checks it again on commit.
- If the process dies, Volume parts stay until the storage expires them, and DBFS temporary files stay.

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

Between two backends:

- files are streamed through this process, `transfer_concurrency` at a time;
- a recursive move deletes each source file as soon as it is copied, then the folders left empty;
- monitors see a copy or move between backends as one operation of `DatabricksFileSystem`.

---

## Inside a Databricks workspace

On a Databricks cluster or notebook, file operations use the local file system by default:

- `/Volumes/...` and `/Workspace/...` are the same local paths.
- Legacy DBFS paths are under `/dbfs`, e.g. `dbfs:/data/file` is `/dbfs/data/file`.

Paths returned, e.g. by `ls`, are the same as outside Databricks.

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

Reads and writes run concurrent requests. Large files are uploaded in parts, sent concurrently. Files go through the
endpoints that the workspace names, as the Databricks SDK uses them.

Reads and writes retry after transient server errors. A file replaced while it is read fails with an `OSError`.

A copy reads the file and writes it again, as the Files API cannot copy. It uses the options for reads and writes. A
move is a copy, then a delete. It replaces an existing file at the destination.

### Modes

| Mode  | Supported | Notes                            |
|-------|-----------|----------------------------------|
| `rb`  | Yes       |                                  |
| `wb`  | Yes       |                                  |
| `xb`  | Yes       | Fails if the file already exists |
| `ab`  | No        |                                  |
| `r+b` | No        |                                  |

### Options for Volumes

Pass these to `DatabricksFileSystem` or to `VolumeFileSystem`:

| Parameter               | Description                                                                       | Default |
|-------------------------|-----------------------------------------------------------------------------------|---------|
| `operation_concurrency` | Maximum concurrent API requests of a recursive operation, such as `rm` or `copy`. | `32`    |
| `io_concurrency`        | Maximum concurrent range reads and part uploads, shared by all the files opened.  | `16`    |

`VolumeFileSystem` also takes `connection_pool_size`, the maximum number of HTTP connections. It defaults to `100`.

Files size their requests by how they are read or written:

- reads fetch blocks of 64 KB to 8 MB;
- sequential reads fetch up to 256 MB ahead, for each file opened;
- files from 5 MB are uploaded in parts of 5 MB to 16 MB.

For a slow or distant network, lower the concurrency:

```python
fs = DatabricksFileSystem(operation_concurrency=4, io_concurrency=4)
```

`VolumeFileSystem` is an async fsspec file system. Its `open_async()` returns files whose methods are awaited.

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

| Mode  | Supported | Notes                            |
|-------|-----------|----------------------------------|
| `rb`  | Yes       |                                  |
| `wb`  | Yes       |                                  |
| `ab`  | Yes       |                                  |
| `xb`  | Yes       | Fails if the file already exists |
| `r+b` | No        |                                  |

---

## Legacy DBFS

Paths other than `/Volumes/...` and `/Workspace/...`.

```python
fs.ls("dbfs:/mnt/my-mount")

with fs.open("dbfs:/mnt/my-mount/data.csv", "rb") as f:
    data = f.read()
```

- A move is the API's own, one request even for a folder.
- `rmdir` refuses a folder that is not empty.
- Reads fetch ranges of up to 1 MB concurrently, limited by `io_concurrency`.
- Files are written in order: `ab` and `r+b` are not supported.
- On a workspace without legacy DBFS, DBFS paths raise `PermissionError`, and `ls("/")` lists `/Volumes` and
  `/Workspace`.

Databricks has deprecated legacy DBFS. The README's [Status](README.md#status) says how its support is tested.

---

## Limitations

### No directory cache

No file system of this package caches a listing: every call reaches Databricks, and `use_listings_cache` is always
`False`. We have no plan to implement fsspec's `dircache` listing cache for now.

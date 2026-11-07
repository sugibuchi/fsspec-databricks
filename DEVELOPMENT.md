# Development

## Layout

| Path                                | Contents                                                                         |
|-------------------------------------|----------------------------------------------------------------------------------|
| `src/fsspec_databricks/__init__.py` | The public API: `DatabricksFileSystem` and `use()`.                              |
| `src/fsspec_databricks/spec.py`     | `DatabricksFileSystem`, which routes each call to a backend.                     |
| `src/fsspec_databricks/base.py`     | The client handling, and the base classes of the router and the backends.        |
| `src/fsspec_databricks/volume.py`   | `VolumeFileSystem`, Unity Catalog Volumes, and its file objects.                 |
| `src/fsspec_databricks/workspace.py` | `WorkspaceFileSystem`, Workspace files and notebooks, and its file object.      |
| `src/fsspec_databricks/dbfs.py`     | `DBFS`, the legacy DBFS.                                                         |
| `src/fsspec_databricks/local.py`    | `DatabricksLocalFileSystem`, the local file system used inside a workspace.      |
| `src/fsspec_databricks/_contrib/strategy/` | A copy of fsspec-alt-strategies: the tree strategies and the file objects.  |
| `src/fsspec_databricks/path.py`     | Parsing `dbfs:/` URLs and POSIX paths, and choosing their backend.               |
| `src/fsspec_databricks/error.py`    | `OSError`s, and the mapping of the SDK's exceptions to them.                     |
| `src/fsspec_databricks/utils.py`    | Timestamps, enums, and what a file mode allows.                                  |
| `tests/`                            | The tests, a file per module, and the fake Files API.                            |

The runtime dependencies are fsspec, databricks-sdk, aiohttp and fsspec-alt-strategies-common.

All Python commands go through [uv](https://docs.astral.sh/uv/).

## Setup

```bash
uv sync
```

Most tests run against a real Databricks workspace. They need its credentials and two work directories in it.

### Work directories

Create a folder in a Unity Catalog Volume and one in the Workspace files. Set their **POSIX paths**, without `dbfs:/`,
in these environment variables:

| Location             | Environment variable                    | Default                                                      |
|----------------------|-----------------------------------------|--------------------------------------------------------------|
| Unity Catalog Volume | `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT`    | `/Volumes/fsspec_databricks_test/fsspec_dbx_test/test/local` |
| Workspace files      | `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` | `/fsspec-databricks-test/local`                              |

The tests delete everything in these folders at the start of each session.

### Credentials

Configure [Databricks Unified Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth), e.g. a
default profile or `DATABRICKS_*` environment variables.

The tests load a `.env` file in the repository folder, through python-dotenv. It can hold the credentials and the two
variables above.

## Tests

```bash
uv run pytest                                   # all tests
uvx ruff check .                                # lint
uvx ruff format --check .                       # formatting
uvx pre-commit run --all-files                  # the hooks
```

| File                       | Tests of                                          | Workspace |
|----------------------------|---------------------------------------------------|-----------|
| `test_spec_fs.py`          | `DatabricksFileSystem`                            | Mostly    |
| `test_base_fs.py`          | `AbstractDatabricksFileSystem`                    | Yes       |
| `test_volume_fs.py`        | `VolumeFileSystem`                                | Mostly    |
| `test_volume.py`           | The Volume file objects                           | No        |
| `test_workspace_fs.py`     | `WorkspaceFileSystem`                             | Yes       |
| `test_workspace.py`        | `WorkspaceFileSystem`, on a fake Workspace API    | No        |
| `test_dbfs.py`             | `DBFS`, on a fake DBFS API                        | No        |
| `test_path.py`             | `path.py`                                         | No        |
| `test_utils.py`            | `utils.py`                                        | No        |

Without credentials, run the files that need no workspace, and the tests of the others that take no workspace fixture.

The fixtures, in `tests/conftest.py`:

- `client`: a `WorkspaceClient` with the default authentication;
- `volume_test_root` and `workspace_test_root`: the work directories, emptied;
- `dummy_api` and `dummy_api_context`: the fake Files API, and its state;
- `client_event_loop` and `aiohttp_session`: fsspec's event loop, and a session on it;
- `dummy_table_parquet`: Parquet bytes of a generated table.

`tests/dummy_api.py` is a FastAPI app of the Files API's endpoints, served by Hypercorn on a free local port. It
covers downloads, single uploads, multipart and resumable uploads, presigned URLs and aborts.
`dummy_api_context.upload_mode` chooses which upload API the fake server answers with.

`tests/utils.py` has the helpers that test files share, such as the fake Workspace API, `FakeWorkspaceAPI`.
`tests/test_dbfs.py` has the fake DBFS API, `FakeDbfsAPI`.

## CI

The workflows are in `.github/workflows/`:

| Workflow          | Runs on               | Does                                                                     |
|-------------------|-----------------------|--------------------------------------------------------------------------|
| `on_push.yaml`    | Pushes                | `check.yaml`, then `test.yaml` for each Python version and workspace.    |
| `check.yaml`      | Called                | ruff check and ruff format check, with the latest ruff.                  |
| `test.yaml`       | Called, or by hand    | pytest in one GitHub environment, optionally with coverage to Codecov.  |
| `on_release.yaml` | Published releases    | `publish.yaml` to PyPI.                                                  |
| `publish.yaml`    | Called                | `uv build` and `uv publish`.                                             |

`on_push.yaml` runs these test jobs:

- Python 3.14 on the `default` environment, with coverage on `main`;
- on `main`, the other supported Python versions on `default`;
- Python 3.14 on `azure`, `aws` and `gcp`, each when the `TEST_ENVIRONMENTS` variable names it.

Each run's work directories are `<root>/<branch>/<Python version>` under the environment's roots.

### Supply chain

The test jobs hold the workspaces' credentials, so what they run is fixed:

- `uv.lock` is committed, with every package's hash. CI installs with `uv sync --locked`, which fails if the lock is
  out of date.
- Actions are pinned to commit SHAs. setup-uv installs only uv versions whose checksums it knows (`latest-known`).
- Dependabot updates `uv.lock` and the Actions' SHAs weekly.
- Workflows run only on pushes and releases, never on pull requests from forks.
- Each workflow's token can only read the repository. Publishing adds `id-token: write`, for PyPI's trusted
  publishing.
- The release build uses no cache.

ruff, run with `uvx`, is the exception: CI runs its latest version, in a job without secrets.

### Environments

Each GitHub environment is a Databricks workspace. Create at least `default`, and `azure`, `aws` or `gcp` as needed.
The tests authenticate as a service principal with read-write access to the work directories.

Every environment has these:

| Name                                               | Value                                          |
|----------------------------------------------------|------------------------------------------------|
| `DATABRICKS_HOST` (secret)                         | The workspace's URL                            |
| `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT` (variable)    | The POSIX path of the Volume work directory    |
| `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` (variable) | The POSIX path of the Workspace work directory |

The variable `DATABRICKS_AUTH_TYPE` chooses how the tests authenticate:

| `DATABRICKS_AUTH_TYPE` | Authentication                                        | Also needs                                                       |
|------------------------|-------------------------------------------------------|------------------------------------------------------------------|
| Not set                | The service principal's client secret                 | `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` (secrets)  |
| `github-oidc`          | Databricks federation policy                          | `DATABRICKS_CLIENT_ID` (secret), `DATABRICKS_TOKEN_AUDIENCE`     |
| `github-oidc-azure`    | Federated credential of a Microsoft Entra ID app      | `ARM_CLIENT_ID`, the app's ID                                    |

Prefer federation: it needs no stored secret. GitHub's OIDC token identifies each environment by the subject
`repo:sugibuchi/fsspec-databricks:environment:<name>`. The federation's trust must name that subject and the issuer
`https://token.actions.githubusercontent.com`.

- **`github-oidc`**: a federation policy on the service principal, in the Databricks account console. Its audience is
  `DATABRICKS_TOKEN_AUDIENCE`, e.g. the account ID.
  The SDK's `github-oidc` exchanges a new token for each request, which slows the tests down.
  So `.github/scripts/with-id-token-file.sh` keeps GitHub's token in a file and runs the tests with `file-oidc`, which
  caches the exchanged token.
- **`github-oidc-azure`**: a federated credential on the Entra ID app, with the audience `api://AzureADTokenExchange`.
  The app is the workspace's service principal. This needs no access to the account console.

With federation, the client secret is not passed to the tests, even if the environment still has one.

The environments are:

| Environment | Workspace                        | Authentication      |
|-------------|----------------------------------|---------------------|
| `default`   | Databricks Free Edition          | Client secret       |
| `azure`     | Azure Databricks                 | `github-oidc-azure` |
| `gcp`       | Databricks on Google Cloud       | `github-oidc`       |

At the repository level:

| Name                          | Value                                                                    |
|-------------------------------|--------------------------------------------------------------------------|
| `CODECOV_TOKEN` (secret)      | The repository's upload token for [Codecov](https://about.codecov.io/)   |
| `TEST_ENVIRONMENTS` (variable) | The environments tested on each push, e.g. `azure gcp`                  |

## Releasing

The version comes from the git tag, through hatch-vcs.

1. Create a GitHub release with a new tag `vX.Y.Z` on `main`.
2. Publishing it runs `on_release.yaml`. It builds the package and publishes it to PyPI, in the GitHub environment
   `pypi`.

`uv publish` authenticates to PyPI with the workflow's OIDC token, through trusted publishing.

Repository rules protect the release path:

- The ruleset "Release tags" lets only repository admins create, move or delete `v*` tags.
- The `pypi` environment deploys only from `v*` tags.
- The ruleset "Default branch" blocks force-pushes to `main` and its deletion.

## Design

### One file system for all of Databricks' paths

`DatabricksFileSystem` takes the paths Databricks itself takes: `dbfs:/` URLs and POSIX paths. It sends each call to
the backend of its path, with the same rules as Databricks:

| Path                                           | Backend               |
|------------------------------------------------|-----------------------|
| `/Volumes/<catalog>/<schema>/<volume>/...`     | `VolumeFileSystem`    |
| `/Workspace/...`                               | `WorkspaceFileSystem` |
| Any other                                      | `DBFS`, if enabled    |

The same code then runs inside and outside Databricks.

### Inside a workspace, the local file system

On a Databricks cluster, `DATABRICKS_RUNTIME_VERSION` is set. `DatabricksFileSystem` then serves every path from the
local file system, unless `use_local_fs_in_workspace=False`.

### The SDK for metadata, HTTP for Volume data

Listings, metadata and changes go through the Databricks SDK. Volume file contents go through the Files API directly,
with aiohttp, so that ranges and parts run concurrently.

### Standard exceptions

Errors are `OSError` subclasses, as from local files: `FileNotFoundError`, `FileExistsError`, `IsADirectoryError`,
`NotADirectoryError` and `PermissionError`. A failed transfer is an `OSError` with `errno.EIO`, caused by the original
exception.

### Picklable

File systems are pickled to run in other processes, e.g. by Dask. fsspec pickles a file system as its constructor
arguments, and unpickling calls the constructor again:

- clients, event loops, threads and sessions are not pickled, and are made again on first use;
- attributes set after construction are lost;
- every constructor argument must be picklable.

A `WorkspaceClient` given as `client` is not picklable. Other ways of authenticating are.

### Known limits

- **Multipart upload is not in the REST reference.** The Databricks SDK for Python uses the same endpoints, as an
  experimental feature, in `databricks/sdk/mixins/files.py`. Compare with it when the API changes.
- **Workspace files are at most 10 MB.** The Workspace API's limit.
- **No append or update modes on Volumes.** `ab` and `r+b` are refused.
- **No listing cache.** Each call reaches the workspace.

## Architecture

- **`DatabricksFileSystem`**, in `spec.py`, routes calls to backends, created on first use.
- **The backends** wrap one Databricks API each. Each is an async `TreeFileSystem` of fsspec-alt-strategies. Each
  subclasses `DatabricksTreeFileSystem`, in `base.py`.
- **The file objects** are the library's. Volume files read and write ranges concurrently. Workspace files are
  sync `WorkspaceFile`s of `workspace.py`, transferred whole. DBFS files are the SDK's own.
- **Paths**, in `path.py`, are parsed with regular expressions into each backend's parts.

### Routing

`DatabricksFileSystem._resolve(path)` gives the file system for a path, and the path to pass to it:

- the local file system and the local path, when running in a workspace with `use_local_fs_in_workspace`;
- else the Volume, Workspace or legacy DBFS backend of `path.fs_type`, and the path unchanged;
- `FileNotFoundError` for legacy DBFS paths when the workspace disables legacy DBFS.

Every method calls `_resolve`. With the local file system, the names returned are mapped back to DBFS paths.

`find`, `glob`, `du`, `walk`, `rm`, and `copy` and `mv` within one backend, are the backend's own. So they run its
tree strategies, which monitors of fsspec-alt-strategies see as calls to the backend. Two cases fall back to fsspec's
own methods on top of the router:

- a path or pattern at the root, which spans the backends;
- a copy or move between backends.

Whether legacy DBFS is enabled is checked once:

- with the local file system, by whether `legacy_dbfs_local_root` exists;
- otherwise, from the workspace setting `disable_legacy_dbfs`.

When legacy DBFS is disabled, `ls("/")` and `info("/")` answer without a backend: the root holds `/Volumes` and
`/Workspace`.

The backends are created on first use, with the file system's client, config and options. `close()` closes them.

Transfers between backends:

- `cp_file` streams the file from one backend's file object into the other's, 8 MB at a time. A folder is created,
  not copied.
- `copy` is fsspec's: it finds the files with the source backend's `find`, then calls `cp_file` for each.
- `mv` is fsspec's: a copy, then a delete.

### The base classes

`DatabricksClientSupport`, a mixin, holds the client handling that every file system shares:

- the Databricks SDK `Config` and `WorkspaceClient`, made on first use from `client`, `config` or `config_params`;
- `close()`, also called at exit and on garbage collection;
- the `dbfs` protocol: `_strip_protocol` and `unstrip_protocol` use `path.py`.

`AbstractDatabricksFileSystem` is the router's base: a synchronous fsspec file system with the mixin.

`DatabricksTreeFileSystem` is the base of the Volume, Workspace and DBFS backends: a `TreeFileSystem` with the mixin.

- `_sdk` runs an SDK call in a thread, with the library's `_blocking`. Each backend has
  `operation_concurrency + io_concurrency` threads of its own, and `close()` shuts them down.
- `_mkdir` and `_makedirs` check the preconditions, then call the subclass's `_create_directory`.
- `operation_concurrency` and `io_concurrency` default to 16 and 8.
- `open` writes text with `"\n"` newlines by default.

Volumes' files take their limit from `io_concurrency`. Workspace and DBFS files do not: the threads are its only use
there.

### Paths

`path.py` matches paths with three regular expressions: `dbfs:/` URLs, Workspace paths and Volume paths.

- `strip_scheme` and `unstrip_scheme` convert between `dbfs:/` URLs and POSIX paths.
- `fs_type` gives a path's backend.
- `parse_volume_path` gives the catalog, schema, volume and path in the volume. Missing parts are `None`.
- `parse_workspace_path` and `parse_dbfs_path` give the path in their API.
- `to_local_path` and `from_local_path` convert to and from local paths on Databricks compute.

### Errors

`error_mapping(path)` is a context manager around SDK calls. It maps the SDK's exceptions:

| SDK exception                           | Raised                              |
|-----------------------------------------|-------------------------------------|
| `NotFound`                              | `FileNotFoundError`                 |
| `AlreadyExists`, `ResourceAlreadyExists`| `FileExistsError`                   |
| `Unauthenticated`, `PermissionDenied`   | `PermissionError` with `EACCES`     |

Other exceptions pass through. The helpers `file_not_found_error` and the like make `OSError`s with the path and the
system's message.

### Legacy DBFS

`DBFS`, in `dbfs.py`, uses the SDK's `WorkspaceClient.dbfs`.

- `_mv_file` is the API's move, which moves a directory with all in it. So `storage_profile` declares
  `renames_folders=True`, and a recursive `mv` moves each folder whole where it can.
- A file moved alone is one move too.
- `_rm_tree` is the API's recursive delete. A recursive `rm` deletes each directory it is given by it, in one request.
- `rmdir` refuses a directory that is not empty. It deletes with the API's non-recursive delete, which the SDK's
  `dbfs.delete` refuses for directories.
- Files are opened with the SDK's `dbfs.open`. `open_async` wraps them in the library's `SyncFileAdapter`, adding the
  mode and size that they do not tell.

### Volumes

`VolumeFileSystem`, in `volume.py`, is an async fsspec file system: it subclasses `DatabricksTreeFileSystem`, a
`TreeFileSystem` of the copy of fsspec-alt-strategies. It implements the primitives, such as `_ls`, `_info`,
`_rm_file`, `_rmdir`, `_create_directory` and `_cp_file`. It adds an aiohttp session, closed by `close()`. Its
`io_concurrency` defaults to 24.

The recursive operations are the library's tree strategies: `find`, `walk`, `du`, `glob`, and recursive `rm`, `copy`
and `mv`.

fsspec mirrors the async methods as synchronous ones, which run on fsspec's event loop. `DatabricksFileSystem` calls
those. The SDK's calls block, so they run in the file system's own threads, with `_sdk`. The Workspace and DBFS
backends work the same way.

The APIs called depend on the depth of the path:

| Path                                   | `info`                                                    | `ls`                            |
|----------------------------------------|-----------------------------------------------------------|---------------------------------|
| `/Volumes`                             | None: a fixed folder                                      | `catalogs.list`                 |
| `/Volumes/<catalog>`                   | `catalogs.get`                                            | `schemas.list`                  |
| `/Volumes/<catalog>/<schema>`          | `schemas.get`                                             | `volumes.list`                  |
| `/Volumes/<catalog>/<schema>/<volume>` | `volumes.read`                                            | `files.list_directory_contents` |
| Deeper                                 | `files.get_metadata`, else `files.get_directory_metadata` | `files.list_directory_contents` |

`ls` of a file returns the file itself, from `files.get_metadata` after the listing fails.

`include_browse` is passed to the Unity Catalog APIs.

Each metadata call is a round trip, so the checks inside a volume make as few as they can:

| Check                                         | Calls                                           |
|-----------------------------------------------|-------------------------------------------------|
| `info` of a file, so also `size` and reading  | 1: `files.get_metadata`                         |
| `info` of a directory                         | 2: `files.get_metadata`, then the directory's   |
| `isdir`                                       | 1: `files.get_directory_metadata`               |
| `isfile`                                      | 1: `files.get_metadata`                         |
| Opening for writing                           | 1: `isdir`, to refuse directories               |
| `ls` of a directory                           | 1: `files.list_directory_contents`              |

`DatabricksFileSystem` passes `isdir` and `isfile` to the backends. `test_volume_fs_metadata_calls` counts the calls.

Copies, moves and deletes inside a volume:

- `cp_file` streams the file from a `VolumeReadableFile` into a writable Volume file, as the Files API has no copy.
- `mv` is a copy, then a delete: the Files API has no rename.
- `rm` of a file calls `files.delete`. Only when that fails does it check for a directory.
- A recursive `rm`, `copy` or `mv` is a tree strategy. It acts on each folder as soon as it is listed.
- Catalogs, schemas and volumes are never deleted or created as folders.

`storage_profile` is `StorageProfile()`: nothing about the storage is declared, so the strategies take the way that is
right on any storage.

Two limits bound the requests, each an `asyncio.Semaphore`:

- `operation_concurrency`, the requests of the recursive operations;
- `io_concurrency`, the range reads and part uploads of all the files opened.

`test_volume_fs_rm_calls` and `test_volume_fs_cp_file_streams` check these on a fake `WorkspaceClient.files`.

#### The HTTP session

One aiohttp `ClientSession` serves the file system. Its `TCPConnector` allows `connection_pool_size` connections.

- The session is created on the loop that first uses it, as `TCPConnector` needs a running loop. It is never pickled.
- `close()` closes the session on its loop.
- Requests are authenticated by an aiohttp middleware, with the SDK `Config`'s headers.

#### Storage proxy or presigned URLs

On first open, the file system probes the Databricks storage proxy, `storage-proxy.databricks.com`, with the workspace
ID:

- If it answers, all requests go to it.
- Otherwise, requests go to the workspace's host. Data moves through presigned URLs from the Files API.

Files smaller than `min_presigned_url_size`, 1 MB, are read from the Files API directly. Presigned URLs for reads are
renewed after 30 minutes.

#### Reading

`VolumeReadableFile` subclasses the library's `AdaptiveReadableFile`, and implements `_fetch_range` with a range
`GET`. The base class sizes the blocks, from 1 MB to 4 MB, and fetches ahead on sequential reads.

A range request that gets a `429`, `500`, `502`, `503` or `504` is retried up to 3 times, with exponential backoff and
jitter. Writes are not retried: a repeated `PUT` can fail with "already exists" when the first one went through.

#### Writing

`VolumeWritableFile` subclasses the library's `AdaptiveWritableFile`, which buffers the writes:

1. Data is buffered until it reaches `min_multipart_size`, 5 MB. A file closed before then is uploaded by one `PUT`.
2. Then the upload is initiated.
3. Parts are uploaded as the buffer fills, from 5 MB to 16 MB.
4. `close()` uploads the rest, waits for the parts, and completes the upload.

The server's answer to the initiation names the upload API. The file builds a session object of that API from it:

- `_MultipartSession` sends parts concurrently, then completes the upload with their ETags.
- `_ResumableSession` sends one part at a time, with `Content-Range`. The file sets `sequential` for it at the start.
  The last part completes the upload. A `308` answer gives the bytes received, and the rest is sent again.

The file forwards parts, completion and abort to the session. An answer naming neither API raises an `OSError`.

Multipart support follows the endpoint, not the cloud. The Files API's endpoints listing, `GET /api/2.0/fs/endpoints`,
gives each endpoint a `multipart_upload_supported` flag:

- the offload path on the workspace host, which presigned URLs use, supports multipart uploads;
- Files API 3.0 on the storage host supports them;
- Files API 2.0 called directly on the workspace host does not.

AWS and GCP workspaces answered the same, and both started multipart uploads in the live tests. The server's choice is
not fixed per cloud and may change with API versions, so the file follows the server's answer per upload.

If a part fails, `close()` aborts the upload and raises an `OSError`. Azure Databricks refuses aborts of multipart
uploads with a 400 answer, and the abort is then skipped.

Part sizes must be multiples of 256 KB. The constructor refuses other minimum and maximum sizes.

#### The copy of fsspec-alt-strategies

`src/fsspec_databricks/_contrib/strategy/` is a copy of `packages/impl/src/fsspec_alt_strategies/` of
[fsspec-alt-strategies](https://github.com/sugibuchi/fsspec-alt-strategies), from its `wip` branch. It leaves out
`flat/`. Its modules keep their license headers and relative imports, and ruff skips them.

Their shared interface is not copied. It is the dependency `fsspec-alt-strategies-common`, from the same repository's
`packages/common`.

To refresh the copy:

1. Copy `common/`, `files/`, `tree/`, `__init__.py` and `monitor.py` over the old ones.
2. Run `uv lock --upgrade-package fsspec-alt-strategies-common`, so that the interface matches.
3. Run the tests.

### Workspace files

`WorkspaceFileSystem`, in `workspace.py`, uses the SDK's `WorkspaceClient.workspace`. Its `storage_profile` declares
`implicit_folders=False` and `renames_files=False`: directories are objects, and the API has no move.

`WorkspaceFile` is a sync `io.BytesIO` calling the SDK directly, with no event loop:

- Opening for reading or appending downloads the whole file into memory.
- Closing uploads it whole, if changed. Leaving a `with` block by an exception uploads nothing.
- `open_async()` opens it in a thread, and wraps it in the library's `SyncFileAdapter`.
- Exclusive creation uploads without overwriting, so that the API refuses a file created meanwhile.
- Files are at most 10 MB.

Notebooks are objects of type `NOTEBOOK`. `notebook_format` gives their export and import format, `SOURCE` by default.

- An existing notebook keeps its language.
- A new notebook needs `language`.
- Other files use `AUTO` export and `RAW` import, and refuse `notebook_format`.

- `cp_file` downloads and uploads, keeping notebooks as notebooks, and overwrites the destination.
- `_rm_tree` is the API's recursive delete, which a recursive `rm` uses on each directory it is given.
- `rm` of a file checks its type first, as the API deletes an empty directory as a file.

### File objects

Every backend's files are the library's file objects, or the SDK's for DBFS. The library's raise `RuntimeError` when
their synchronous methods are called from the event loop. Code on the loop uses `open_async()` and awaits the file's
methods.

### Transactions

fsspec opens a file with `autocommit=False` inside `with fs.transaction:`. Each backend's `_open` then returns the
library's `TransactionFile`, with a `FileTransaction` of `files/fsspec_transaction.py`:

- Volumes use `AdaptiveWritableTransaction`, with `hold_last_part=True`. A resumable upload completes with its last
  part, and the server chooses that API only once the upload starts.
- Workspace files use `_WorkspaceTransaction`, in `workspace.py`. Commit closes the `WorkspaceFile`, which uploads it,
  and discard drops it.
- DBFS uses `TempPathTransaction`. Commit deletes the destination, then moves the temporary file, as the API's move
  refuses an existing destination.

`DatabricksFileSystem.open` passes `autocommit` to the backend. It then moves the file from the backend's own
transaction to its own, which commits or discards the files of every backend.

### Local file system

`DatabricksLocalFileSystem`, in `local.py`, is fsspec's `LocalFileSystem` with the `dbfs` protocol. It gets local
paths from `DatabricksFileSystem`.

`path.to_local_path` gives a path's local path on Databricks compute:

- Volume and Workspace paths are the same.
- Legacy DBFS paths are under `legacy_dbfs_local_root`, `/dbfs` by default.

`path.from_local_path` is the reverse, for the names that `ls` and `info` return.

### Logging

Logging follows s3fs and gcsfs. Each module has a logger of its name, under `fsspec_databricks`, and no handlers are
installed.

- Each request to Databricks, except range reads, is one DEBUG line: its API or HTTP method, the path and the outcome.
- `_sdk` logs the SDK's calls, and `volume.py` logs its own HTTP requests.
- Reads of ranges are not logged: a read makes many. Their retries are still WARNINGs.
- Retries and aborted uploads are WARNINGs.
- The fallback from the storage proxy is DEBUG: outside Databricks, the proxy is normally absent.
- Operations, such as `find` or `rm`, are not logged here. The library's monitors report them.

# Development

## Layout

| Path                                | Contents                                                                         |
|-------------------------------------|----------------------------------------------------------------------------------|
| `src/fsspec_databricks/__init__.py` | The public API: `DatabricksFileSystem` and `use()`.                              |
| `src/fsspec_databricks/spec.py`     | `DatabricksFileSystem`, the library's router, sending each call to a backend.    |
| `src/fsspec_databricks/base.py`     | The client handling, and the base classes of the backends.                       |
| `src/fsspec_databricks/volume.py`   | `VolumeFileSystem`, Unity Catalog Volumes, and its file objects.                 |
| `src/fsspec_databricks/workspace.py` | `WorkspaceFileSystem`, Workspace files and notebooks, and its file object.      |
| `src/fsspec_databricks/dbfs.py`     | `DBFS`, the legacy DBFS.                                                         |
| `src/fsspec_databricks/local.py`    | `DatabricksLocalFileSystem`, used inside a workspace, and its async wrapper.     |
| `src/fsspec_databricks/_contrib/strategy/` | A copy of fsspec-alt-strategies: the tree strategies and the file objects.  |
| `src/fsspec_databricks/path.py`     | Parsing `dbfs:/` URLs and POSIX paths, and choosing their backend.               |
| `src/fsspec_databricks/error.py`    | `OSError`s, and the mapping of the SDK's exceptions to them.                     |
| `src/fsspec_databricks/utils.py`    | Timestamps, enums, and what a file mode allows.                                  |
| `tests/`                            | The tests, a file per module, and the dummy API server.                          |

The runtime dependencies are fsspec, databricks-sdk, aiohttp and fsspec-alt-strategies-common.

All Python commands go through [uv](https://docs.astral.sh/uv/).

## Setup

```bash
uv sync
```

The tests run on a local dummy server by default, with no workspace and no credentials. To run them on a real
Databricks workspace too, set up its credentials and work directories as below.

### Work directories

Create a folder in a Unity Catalog Volume and one in the Workspace files. Set their **POSIX paths**, without `dbfs:/`,
in these environment variables:

| Location             | Environment variable                    | Default                                                      |
|----------------------|-----------------------------------------|--------------------------------------------------------------|
| Unity Catalog Volume | `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT`    | `/Volumes/fsspec_databricks_test/fsspec_dbx_test/test/local` |
| Workspace files      | `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` | `/fsspec-databricks-test/local`                              |
| Legacy DBFS          | `FSSPEC_DATABRICKS_DBFS_TEST_ROOT`      | `/fsspec-databricks-test/local`                              |

The tests delete everything in these folders at the start of each session.

### Credentials

Configure [Databricks Unified Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth), e.g. a
default profile or `DATABRICKS_*` environment variables.

The tests load a `.env` file in the repository folder, through python-dotenv. It can hold the credentials and the
variables above.

## Tests

```bash
uv run pytest                                   # all tests, on the dummy server only
uv run pytest --api=live                        # the backend tests on the live workspace
uv run pytest --api=both                        # the backend tests on both
uv run pytest --fs=volume,workspace             # only these backends: volume, workspace, dbfs
uvx ruff check .                                # lint
uvx ruff format --check .                       # formatting
uvx pre-commit run --all-files                  # the hooks
```

Two options of `tests/conftest.py` choose what runs:

- `--api` chooses where the backend tests run: `dummy`, the default, `live` or `both`.
- `--fs` chooses the backends tested, comma-separated or repeated. By default, all three are.

The options combine, e.g. `--api=live --fs=volume`. A test id names its backends and its API, e.g.
`test_dbfs_rm[dbfs-dummy-DBFS]`.

| File                | Tests of                                          |
|---------------------|---------------------------------------------------|
| `test_spec.py`      | `DatabricksFileSystem` and its routing            |
| `test_base.py`      | `BaseDatabricksFileSystem` and the client mixin   |
| `test_volume.py`    | `VolumeFileSystem` and its file objects           |
| `test_workspace.py` | `WorkspaceFileSystem`                             |
| `test_dbfs.py`      | `DBFS`                                            |
| `test_dummy_api.py` | The dummy server, through the SDK                 |
| `test_path.py`      | `path.py`                                         |
| `test_utils.py`     | `utils.py`                                        |

A backend test takes the `client` fixture, or one built on it. It runs once per API chosen:

- `dummy`, on the dummy server;
- `live`, on the real workspace, skipped without working credentials.

The live DBFS tests also skip when the workspace disables legacy DBFS. Most workspaces do, so the dummy is usually how
DBFS is tested.

Some tests make sense on one side only:

- A test taking the `dummy` fixture runs on the dummy server only. It switches the server's behaviour, or counts the
  requests it served.
- The `dummy_only` and `live_only` markers restrict the others.
- A test is skipped when `--api` excludes its side.

Each test of a backend has a `backend` marker, e.g. a module's `pytestmark = pytest.mark.backend("volume")`. A test
spanning backends names all of them, and runs only when `--fs` selects them all. Tests of no backend always run.

Tests without an API, and dummy cases, never need credentials. An autouse fixture points the default client
configuration at the dummy server, so `WorkspaceFileSystem()` and the like never reach a workspace.

The fixtures, in `tests/conftest.py`:

- `client`: the `WorkspaceClient` of the API the test runs on;
- `dbfs_client`: the same, skipped where legacy DBFS is disabled;
- `volume_test_root`, `workspace_test_root` and `dbfs_test_root`: the work directories, emptied;
- `dummy`: the dummy server's state and the requests it served, reset after the test;
- `dummy_api` and `dummy_client`: the dummy server's URL, and the SDK's client of it;
- `client_event_loop` and `aiohttp_session`: fsspec's event loop, and a session on it;
- `dummy_table_parquet`: Parquet bytes of a generated table.

`tests/utils.py` has the helpers that test files share, such as `init_volume_test_dir`, which fills a directory named
after the test. Test files never import each other.

### The dummy server

`tests/dummy_api.py` is a FastAPI app served by Hypercorn on a free local port. It answers these APIs, as their REST
references describe:

- the Files API;
- the Workspace API;
- the legacy DBFS API;
- the Unity Catalog API's reads of catalogs, schemas and volumes.

The SDK's own `WorkspaceClient` talks to it over HTTP, so the SDK's requests, errors and mixins run as against a
workspace.

- The Files API covers directories, downloads with ranges, uploads whole or in parts, presigned URLs and aborts.
- Parts go to presigned URLs, or to the Files API itself, as through the storage proxy.
- `dummy` holds the Volumes' files, the Workspace files and the DBFS root, separate as in Databricks.
- `dummy.requests` records each request served: its method, its path, and its query and body parameters without data.
  `dummy.requests_to(prefix, method)` filters them. Tests counting requests assert on these.
- `test_dummy_api.py` checks the server through the SDK alone, without this package.

Switches of `dummy` make the server answer as a given workspace:

- `upload_mode` chooses the upload API that initiate-upload answers with: `multipart`, `resumable` or `unknown`;
- `presigned_urls_only` refuses data transfers to the Files API itself;
- `abort_supported = False` refuses abort URLs, as Azure Databricks does;
- `read_errors` lists statuses that the next downloads fail with;
- `storage_proxy_available` answers the storage proxy's probe;
- `legacy_dbfs_disabled` answers as a workspace disabling the DBFS root.
- `dbfs_add_block_errors` counts the next DBFS add-blocks that fail.

The references leave some behaviour open. The dummy then makes an assumption, marked in a comment:

- A DBFS delete of a missing path succeeds.
- A DBFS file exists from `create` on, and grows with each block.
- DBFS `create` and `move` create the missing parents.
- DBFS lists are sorted by name, Workspace lists in creation order.
- A Workspace import refuses a missing parent folder.
- Workspace imports and exports support `SOURCE`, `AUTO` and `RAW` only, with no directories.
- Every catalog, schema and volume exists, and paths outside the volumes are invalid parameters.
- Unity Catalog lists name the catalogs, schemas and volumes holding files or directories.
- A Files API upload creates the missing parent directories.
- A file is no directory, and a directory no file: each is not found as the other.
- A file in the way of a directory or an upload fails as an existing file, with `ALREADY_EXISTS`.
- Volume lists are sorted by name, and their directories tell no size or time.
- A non-empty volume directory fails with `DIRECTORY_NOT_EMPTY`, whose message says it "is not empty".
- An unknown upload session is not found, and a resumable chunk with a gap is refused.

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

- **`DatabricksFileSystem`**, in `spec.py`, routes calls to backends, created on first use. It is the library's
  `AsyncRoutingFileSystem`.
- **The backends** wrap one Databricks API each. Each is an async `TreeFileSystem` of fsspec-alt-strategies. Each
  subclasses `BaseDatabricksFileSystem`, in `base.py`.
- **The file objects** are the library's. Volume files read and write ranges concurrently. Workspace files are
  sync `WorkspaceFile`s of `workspace.py`, transferred whole. DBFS files are the SDK's own.
- **Paths**, in `path.py`, are parsed with regular expressions into each backend's parts.

### Routing
### Routing

`DatabricksFileSystem` subclasses the library's `AsyncRoutingFileSystem`, in `common/routing.py` of the copy. The library
passes each call on, and `spec.py` only says where each path goes:

- `_route(path)` gives the backend and its path: the local file system and the local path in a workspace with
  `use_local_fs_in_workspace`, else the Volume, Workspace or legacy DBFS backend of `path.fs_type`.
- It gives `None` for the root, which spans the backends.
- It raises `FileNotFoundError` for legacy DBFS paths when the workspace disables legacy DBFS.
- `_outer` maps the local file system's names back to DBFS paths.
- `_roots` lists the root: `/Volumes` and `/Workspace` without legacy DBFS, else the root of legacy DBFS.

The library's router does the rest:

- `find`, `glob`, `du`, `walk`, `rm`, and `copy` and `mv` within one backend, are the backend's own. So they run its
  tree strategies, which monitors see as calls to the backend, with the router in `Call.via`.
- A path or pattern at the root is walked by fsspec's own methods, over `_roots`.
- A change of the root raises `PermissionError`.
- A recursive copy or move between two backends is `tree_cp` or `tree_mv`, as each backend's profile declares
  `fs_type="tree"`, under one call of the router. Each file is streamed through `open_async`, 8 MB at a time,
  `transfer_concurrency` files at a time.
- A move between backends deletes each source file once it is copied, and the source folders left empty.

No two backends share a storage, and Databricks has no copy between them. So `spec.py` keeps the router's
`_cross_fs_cp_file`, and a copy within one backend is its own only when both paths route to it.

Whether legacy DBFS is enabled is checked once:

- with the local file system, by whether `legacy_dbfs_local_root` exists;
- otherwise, from the workspace setting `disable_legacy_dbfs`.

`_route` asks nothing: it routes every legacy DBFS path to the DBFS backend. The backend asks the workspace setting
before its first request, in its own threads. Concurrent first requests wait for the same one. The answer is cached on
the instance, and a copy asks again. Without legacy DBFS, every path of the backend raises `FileNotFoundError`. `_roots`
uses the same cached answer. Only the sync property `legacy_dbfs_enabled` blocks, when it asks first.

The backends are created on first use with the router's `_backend`, with the file system's client, config and
options. `close()` closes them.

The router needs async backends. The local file system is sync, so the library's `SyncTreeWrapper` wraps it.

### The base classes

`DatabricksClientSupport`, a mixin, holds the client handling that every file system shares:

- the Databricks SDK `Config` and `WorkspaceClient`, made on first use from `client`, `config` or `config_params`;
- `close()`, also called at exit and on garbage collection;
- the `dbfs` protocol: `_strip_protocol` and `unstrip_protocol` use `path.py`.

`DatabricksFileSystem` takes the mixin with the library's `AsyncRoutingFileSystem`.

`BaseDatabricksFileSystem` is the base of the Volume, Workspace and DBFS backends: a `TreeFileSystem` with the mixin.

- `_sdk` runs an SDK call in a thread. Each backend has `operation_concurrency` threads of its own, made on first
  use.
- `close()` shuts the threads down, as does collecting the file system.
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
- Files are the library's adaptive files: `DbfsReadableFile` and `DbfsWritableFile`. They call the API in the file
  system's threads, and share its `io_limit`. The SDK's own `dbfs.open` reads and writes one block at a time, and
  cannot seek.
- The API's read, add-block and put each take 1 MB at most. The SDK sends 1 MiB of raw bytes per add-block, so the limit
  is taken to be on the decoded data.
- A read fetches ranges of 1 MB at most, several at once. The API names no version of a file, so a file replaced while
  read is not detected.
- A file of 1 MB at most is written by one put. A larger one is a stream: create, add-blocks in order, close. So the
  writer is `sequential`.
- Create replaces the file there at once, and the file grows with each block. An aborted stream is closed, then its file
  deleted: a failed `wb` write over a file leaves no file.
- A plain file writes to its own path, with no temporary path. Commit would need a delete and a move, as the API's move
  refuses an existing destination, and a crash would leave the temporary file.
- Mode `x` is checked when opening. Create and put check it again, with overwrite off.
- Append mode is refused, as the API's create cannot append.

### Volumes

`VolumeFileSystem`, in `volume.py`, is an async fsspec file system: it subclasses `BaseDatabricksFileSystem`, a
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

`DatabricksFileSystem` passes `isdir` and `isfile` to the backends. `test_volume_metadata_requests` counts the requests.

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

`test_volume_rm_requests` and `test_volume_cp_file_streams` check these on the dummy server's recorded requests.

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
   Delete the modules the library no longer has.
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

Every backend's files are the library's file objects. The library's raise `RuntimeError` when
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
- DBFS cannot hold a last part as Volumes do. Its stream shows the file as it grows, and replaces the file there when it
  starts.

The router's `_open` passes `autocommit` to the backend's `_open`. fsspec's `open` adds the file to the router's own
transaction, which commits or discards the files of every backend. The backends' own transactions stay empty.

The router's `_pipe_file` writes through `open_async`, which knows no transaction. So `DatabricksFileSystem.pipe_file` writes
with `open` inside a transaction.

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

# Development

## Layout

| Path                                       | Contents                                                                  |
|--------------------------------------------|---------------------------------------------------------------------------|
| `src/fsspec_databricks/__init__.py`        | The public API: `DatabricksFileSystem` and `use()`.                       |
| `src/fsspec_databricks/spec.py`            | `DatabricksFileSystem`, the router sending each call to a backend.        |
| `src/fsspec_databricks/base.py`            | The client handling, and the base classes of the backends.                |
| `src/fsspec_databricks/http_client.py`     | The aiohttp session, requests and errors of the Volume and DBFS files.    |
| `src/fsspec_databricks/volume.py`          | `VolumeFileSystem`, Unity Catalog Volumes, and its file objects.          |
| `src/fsspec_databricks/workspace.py`       | `WorkspaceFileSystem`, Workspace files and notebooks, and its file object. |
| `src/fsspec_databricks/dbfs.py`            | `DBFS`, the legacy DBFS, and its file objects.                            |
| `src/fsspec_databricks/local.py`           | `DatabricksLocalFileSystem`, used inside a workspace.                     |
| `src/fsspec_databricks/path.py`            | Parsing `dbfs:/` URLs and POSIX paths, and choosing their backend.        |
| `src/fsspec_databricks/error.py`           | `OSError`s, and the mapping of the SDK's exceptions to them.              |
| `src/fsspec_databricks/utils.py`           | Timestamps, enums, and what a file mode allows.                           |
| `src/fsspec_databricks/_contrib/strategy/` | A copy of fsspec-alt-strategies: the strategies and the file objects.     |
| `tests/`                                   | The tests, a file per module, and the dummy API server.                   |

The runtime dependencies are fsspec, databricks-sdk, aiohttp and fsspec-alt-strategies. All Python commands go through
[uv](https://docs.astral.sh/uv/).

## Setup

```bash
uv sync
```

The tests run on a local dummy server by default, with no workspace and no credentials. To run them on a real
Databricks workspace too, set up its work directories and credentials.

### Work directories

Create a folder in a Unity Catalog Volume and one in the Workspace files. Set their POSIX paths, without `dbfs:`, in
these environment variables:

| Location             | Environment variable                    | Default                                                      |
|----------------------|-----------------------------------------|--------------------------------------------------------------|
| Unity Catalog Volume | `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT`    | `/Volumes/fsspec_databricks_test/fsspec_dbx_test/test/local` |
| Workspace files      | `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` | `/fsspec-databricks-test/local`                              |
| Legacy DBFS          | `FSSPEC_DATABRICKS_DBFS_TEST_ROOT`      | `/fsspec-databricks-test/local`                              |

The tests delete everything in these folders at the start of each session.

### Credentials

Configure [Databricks Unified Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth), e.g. a
default profile or `DATABRICKS_*` environment variables. The tests load a `.env` file in the repository folder, which
can hold the credentials and the variables above.

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

- `--api` chooses where the backend tests run: `dummy`, the default, `live` or `both`;
- `--fs` chooses the backends tested, comma-separated or repeated, all three by default.

Without the options, `FSSPEC_DATABRICKS_TEST_API` and `FSSPEC_DATABRICKS_TEST_FS` choose, from the environment or
`.env`. An option overrides its variable. A test id names its backends and its API, e.g.
`test_dbfs_rm[dbfs-dummy-DBFS]`.

| File                  | Tests of                                        |
|-----------------------|-------------------------------------------------|
| `test_spec.py`        | `DatabricksFileSystem` and its routing          |
| `test_base.py`        | `BaseDatabricksFileSystem` and the client mixin |
| `test_http_client.py` | The mapping of failed requests to `OSError`s    |
| `test_volume.py`      | `VolumeFileSystem` and its file objects         |
| `test_workspace.py`   | `WorkspaceFileSystem`                           |
| `test_dbfs.py`        | `DBFS` and its file objects                     |
| `test_dummy_api.py`   | The dummy server, through the SDK               |
| `test_path.py`        | `path.py`                                       |
| `test_utils.py`       | `utils.py`                                      |

A backend test takes the `client` fixture, or one built on it. It runs once per API chosen: on the dummy server, and
on the live workspace. Live tests are opt in, and once asked for, a workspace that cannot be reached is an error, not
a skip. The live DBFS tests also skip when the workspace disables
legacy DBFS. No test workspace enables it, so DBFS is tested on the dummy server only.

Some tests make sense on one side only:

- A test taking the `dummy` fixture runs on the dummy server only. It switches the server's behaviour, or counts the
  requests it served.
- The `dummy_only` and `live_only` markers restrict the others.
- A test is skipped when `--api` excludes its side.

Each backend test has a `backend` marker, e.g. a module's `pytestmark = pytest.mark.backend("volume")`. A test spanning
backends names all of them, and runs only when `--fs` selects them all. Tests of no backend always run.

Tests on the dummy server never need credentials. An autouse fixture points the default client configuration at the
dummy server, so `WorkspaceFileSystem()` and the like never reach a workspace.

The fixtures of `tests/conftest.py`:

- `client`: the `WorkspaceClient` of the API the test runs on;
- `dbfs_client`: the same, skipped where legacy DBFS is disabled;
- `volume_test_root`, `workspace_test_root` and `dbfs_test_root`: the work directories, emptied;
- `dummy`: the dummy server's state and the requests it served, reset after the test;
- `dummy_api` and `dummy_client`: the dummy server's URL, and the SDK's client of it;
- `client_event_loop` and `aiohttp_session`: fsspec's event loop, and a session on it;
- `dummy_table_parquet`: Parquet bytes of a generated table.

`tests/utils.py` has the helpers that test files share, such as `init_volume_test_dir`. Test files never import each
other.

### The dummy server

`tests/dummy_api.py` is a FastAPI app served by Hypercorn on a free local port. The SDK's own `WorkspaceClient` talks
to it over HTTP, so the SDK's requests, errors and mixins run as against a workspace. It answers these APIs, as their
REST references describe:

- the Files API: directories, downloads with ranges, uploads whole or in parts, presigned URLs and aborts;
- the Workspace API;
- the legacy DBFS API;
- the Unity Catalog API's reads of catalogs, schemas and volumes.

Parts go to presigned URLs, or to the Files API itself, as through the storage proxy. `dummy` holds the Volumes'
files, the Workspace files and the DBFS root, separate as in Databricks. `test_dummy_api.py` checks the server through
the SDK alone, without this package.

`dummy.requests` records each request served: its method, path, and query and body parameters without data.
`dummy.requests_to(prefix, method)` filters them. Tests counting requests assert on these.

Switches of `dummy` make the server answer as a given workspace:

- `upload_mode` chooses the upload API that initiate-upload answers with: `multipart`, `resumable` or `unknown`;
- `presigned_urls_only` refuses data transfers to the Files API itself;
- `abort_supported = False` refuses abort URLs, as Azure Databricks does;
- `read_errors` lists statuses that the next downloads fail with;
- `storage_proxy_available` answers the storage proxy's probe;
- `legacy_dbfs_disabled` answers as a workspace disabling the DBFS root;
- `dbfs_add_block_errors` counts the next DBFS add-blocks that fail.

Where the references leave behaviour open, the dummy makes an assumption, marked in a comment:

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

| Workflow       | Runs on                                   | Does                                               |
|----------------|-------------------------------------------|----------------------------------------------------|
| `ci.yaml`      | Pull requests, pushes to `main`, by hand  | Lint, offline tests, then live tests on each cloud |
| `codeql.yaml`  | Pull requests, pushes to `main`, weekly, by hand | CodeQL scan of the Python code and the workflows |
| `release.yaml` | Pushed `v*` tags                          | Build, GitHub release, and publishing to PyPI      |

`ci.yaml` has three jobs:

- `lint`: ruff check, ruff format check, zizmor and actionlint;
- `test`: pytest `--api=dummy` on Python 3.11 to 3.14, with no credentials, and coverage to Codecov from 3.14;
- `live`: after `test`, pytest `--api=live` on Python 3.14, in the environments `aws`, `azure` and `gcp` in parallel.

`live` runs on pushes, by hand, and on pull requests from branches of this repository. Pull requests from forks and
Dependabot get no environment secrets, so they run `test` only. An environment without `DATABRICKS_HOST` is skipped
with a notice. Each run's work directories are `<root>/<branch>` under the environment's roots.

### Supply chain

The live jobs hold the workspaces' credentials, so what they run is fixed:

- `uv.lock` is committed, with every package's hash. CI installs with `uv sync --locked`, which fails on a stale lock.
- Actions are pinned to commit SHAs, and setup-uv to a uv version.
- Dependabot updates `uv.lock` and the Actions' SHAs weekly.
- No workflow gives secrets to pull requests from forks.
- Each workflow's token can only read the repository. Only the jobs that need them add `id-token: write` or
  `contents: write`.
- The live jobs and the release build use no cache.

ruff, zizmor and actionlint, run with `uvx`, are the exception: CI runs their latest versions, in a job without secrets.

### Environments

Each of the GitHub environments `aws`, `azure` and `gcp` is a Databricks workspace. The tests authenticate as a service
principal with read-write access to the work directories.

| Environment | Workspace                             | Authentication               |
|-------------|---------------------------------------|------------------------------|
| `aws`       | Databricks Free Edition, on AWS       | Client secret                |
| `azure`     | Azure Databricks                      | `github-oidc-azure`          |
| `gcp`       | Databricks on Google Cloud            | `github-oidc`                |

Every environment has these:

| Name                                               | Value                                          |
|----------------------------------------------------|------------------------------------------------|
| `DATABRICKS_HOST` (secret)                         | The workspace's URL                            |
| `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT` (variable)    | The POSIX path of the Volume work directory    |
| `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` (variable) | The POSIX path of the Workspace work directory |
| `FSSPEC_DATABRICKS_TEST_FS` (variable, optional)   | The backends tested, e.g. `volume,workspace`   |
| `FSSPEC_DATABRICKS_TEST_SCOPES` (variable, optional) | The OAuth scopes of the secret, e.g. `files,workspace,unity-catalog` |

The variable `DATABRICKS_AUTH_TYPE` chooses how the tests authenticate:

| `DATABRICKS_AUTH_TYPE` | Authentication                                   | Also needs                                                      |
|------------------------|--------------------------------------------------|-----------------------------------------------------------------|
| Not set                | What the SDK resolves, e.g. a client secret      | `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` (secrets) |
| `github-oidc`          | Databricks federation policy                     | `DATABRICKS_CLIENT_ID` (secret), `DATABRICKS_TOKEN_AUDIENCE`    |
| `github-oidc-azure`    | Federated credential of a Microsoft Entra ID app | `ARM_CLIENT_ID`, the app's ID                                   |

Prefer federation where the workspace allows it: it needs no stored secret. GitHub's OIDC token names each environment
by the subject `repo:sugibuchi/fsspec-databricks:environment:<name>`. The federation's trust must name that subject and
the issuer `https://token.actions.githubusercontent.com`.

- **`github-oidc`**: a federation policy on the service principal, in the Databricks account console, with the
  audience `DATABRICKS_TOKEN_AUDIENCE`, e.g. the account ID. The SDK's `github-oidc` exchanges a new token for each
  request, which slows the tests down. So `.github/scripts/with-id-token-file.sh` keeps GitHub's token in a file, and
  runs the tests with `file-oidc`, which caches the exchanged token.
- **`github-oidc-azure`**: a federated credential on the Entra ID app, with the audience `api://AzureADTokenExchange`.
  The app is the workspace's service principal. This needs no access to the account console.

Keep only the settings of the environment's own authentication: the SDK refuses a configuration that matches several.
The SDK asks for the OAuth scope `all-apis` by default. A secret limited to fewer scopes needs them named in
`FSSPEC_DATABRICKS_TEST_SCOPES`: the tests need `files`, `workspace` and `unity-catalog`, and `settings` for DBFS.

At the repository level, the secret `CODECOV_TOKEN` is the repository's upload token for
[Codecov](https://about.codecov.io/).

## Releasing

The version comes from the git tag, through hatch-vcs.

1. Push a new tag `vX.Y.Z` on `main`, e.g. `git push origin v0.1.0`.
2. `release.yaml` builds the package and checks that its version is the tag's.
3. It creates the GitHub release with the built files, and publishes them to PyPI in the GitHub environment `pypi`.

PyPI's trusted publisher for `fsspec-databricks` names the workflow `release.yaml` and the environment `pypi`. It
authenticates with the workflow's OIDC token, with no stored token.

Repository rules protect the release path:

- The ruleset "Release tags" lets only repository admins create, move or delete `v*` tags.
- The `pypi` environment deploys only from `v*` tags.
- The ruleset "Default branch" blocks force-pushes to `main` and its deletion.

### Release notes

`release.yaml` creates the GitHub release with notes generated from the pull requests. Once it has succeeded, rewrite
the notes for users, e.g. with `gh release edit v0.4.0 --notes-file notes.md`. They hold, in order:

1. An opening line, only when the release as a whole needs one.
2. The sections that apply, in this order:
   - `## New`: each feature as a bold title, a colon, then what it does for the user;
   - `## Changes`: what behaves differently, breaking changes first;
   - `## Fixes`: what was wrong, and is now right.
3. A `Documentation:` list linking the sections of the README, USAGE.md or AUTHENTICATION.md about the changes.
4. `## Install`, with the command to upgrade, and what else to do after upgrading, if anything.
5. The generated sections, kept as they are: `## What's Changed`, `## New Contributors` and `**Full Changelog**`.

The notes describe what users see, not how it is built. They follow the rules for writing docs of CLAUDE.md, and
link files by absolute URL.

## Design

### One file system for all of Databricks' paths

`DatabricksFileSystem` takes the paths Databricks itself takes, `dbfs:/` URLs and POSIX paths, so the same code runs
inside and outside Databricks. It sends each call to the backend of its path:

| Path                                       | Backend               |
|--------------------------------------------|-----------------------|
| `/Volumes/<catalog>/<schema>/<volume>/...` | `VolumeFileSystem`    |
| `/Workspace/...`                           | `WorkspaceFileSystem` |
| Any other                                  | `DBFS`                |

On a Databricks cluster, `DATABRICKS_RUNTIME_VERSION` is set. `DatabricksFileSystem` then serves every path from the
local file system, unless `use_local_fs_in_workspace=False`.

### Strategies from fsspec-alt-strategies

The recursive operations and the file objects come from
[fsspec-alt-strategies](https://github.com/sugibuchi/fsspec-alt-strategies), a library of filesystem-independent
strategies for fsspec. Each backend implements only the primitives of its API, and the library does the rest:

- **Tree strategies:** each backend subclasses `TreeFileSystem`, as all three APIs list one directory per request.
  `find`, `du`, `glob`, `rm`, `copy` and `mv` list many directories at once, and act on files while the listing goes
  on. A listing streams in pages, so a huge directory is never held whole.
- **Storage profiles:** each backend declares what its API does in a `StorageProfile`, e.g. whether an upload creates
  the parent directories. The strategies then skip the requests it makes needless.
- **Router:** `DatabricksFileSystem` is an `AsyncRoutingFileSystem`. It sends each path to its backend, and copies or
  moves files between backends with the source's strategy.
- **File objects:** Volume and DBFS files are adaptive files, whose blocks and parts grow with sequential access. Transactions
  hold a file's writes back until fsspec's transaction commits.
- **Monitors:** the operations behind fsspec's methods report their progress to the monitors of the interface
  package, and can be cancelled.

The strategies are copied into the package rather than installed, so that a release of the library never changes this
package's behaviour. Their shared interface, `fsspec-alt-strategies`, is installed. "The copy of fsspec-alt-strategies"
below says how the copy is kept.

### The SDK for metadata, HTTP for file data

Listings, metadata and changes go through the Databricks SDK. The contents of Volume and DBFS files go through their
REST APIs directly, with aiohttp, so that ranges and parts run concurrently on the event loop.

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
- **Workspace files are at most 10 MB**, the Workspace API's limit.
- **No append or update modes on Volumes.** `ab` and `r+b` are refused.
- **No listing cache.** Each call reaches the workspace.

## Architecture

- **`DatabricksFileSystem`**, in `spec.py`, routes calls to backends created on first use.
- **The backends** wrap one Databricks API each. Each is an async `TreeFileSystem` of fsspec-alt-strategies, and
  subclasses `BaseDatabricksFileSystem`, in `base.py`.
- **The file objects** are the library's. Volume and DBFS files read ranges concurrently. Workspace files are sync
  `WorkspaceFile`s, transferred whole.
- **Paths**, in `path.py`, are parsed with regular expressions into each backend's parts.

### Routing

`DatabricksFileSystem` subclasses the library's `AsyncRoutingFileSystem`, in `router.py` of the copy. The library
passes each call on, and `spec.py` only says where each path goes:

- `_route(path)` gives the backend and its path. In a workspace with `use_local_fs_in_workspace`, it gives the local
  file system and the local path. Otherwise it gives the backend of `path.fs_type`.
- It gives `None` for the root, which spans the backends.
- `_outer` maps the local file system's names back to DBFS paths.
- `_roots` lists the root: the root of legacy DBFS, else `/Volumes` and `/Workspace`.

The library's router does the rest:

- `find`, `glob`, `du`, `walk`, `rm`, and `copy` and `mv` within one backend, are the backend's own. They run its tree
  strategies, which monitors see as calls to the backend, with the router in `Call.via`.
- A path or pattern at the root is walked by fsspec's own methods, over `_roots`.
- A change of the root raises `PermissionError`.
- A recursive copy or move between two backends is `tree_cp` or `tree_mv`, under one call of the router. Each file is
  streamed through `open_async`, 8 MB at a time, `transfer_concurrency` files at a time.
- A move between backends deletes each source file once it is copied, then the source folders left empty.

No two backends share a storage, and Databricks has no copy between them. So `spec.py` keeps the router's
`_cross_fs_cp_file`, and a copy within one backend is its own only when both paths route to it.

No request checks whether legacy DBFS is enabled:

- The DBFS backend sends each request as it comes. On a workspace without legacy DBFS, `error_mapping` makes the API's
  403 a `PermissionError`.
- With the API, `_roots` lists the DBFS root, and falls back to `/Volumes` and `/Workspace` on that `PermissionError`.
- With the local file system, `_roots` falls back when `legacy_dbfs_local_root` does not exist.
- Only the property `legacy_dbfs_enabled` asks the workspace setting `disable_legacy_dbfs`, once per instance,
  blocking. Nothing internal calls it.

`close()` closes the backends. The router needs async backends, so the library's `SyncTreeWrapper` wraps the local
file system.

### The base classes

`DatabricksClientSupport`, a mixin, holds the client handling that every file system shares:

- the Databricks SDK `Config` and `WorkspaceClient`, made on first use from `client`, `config` or `config_params`;
- `close()`, also called at exit and on garbage collection;
- the `dbfs` protocol: `_strip_protocol` and `unstrip_protocol` use `path.py`.

`BaseDatabricksFileSystem` is the base of the Volume, Workspace and DBFS backends. It is a `TreeFileSystem` with this
mixin and the library's `SyncClientSupport`:

- SDK calls run in the file system's own threads, `operation_concurrency` of them, made on first use. `_run_in_thread`
  takes the SDK's bound method and its arguments. It overrides the library's to log each call.
- `_iter_in_thread` takes an SDK listing a batch at a time, in those threads. A backend's `_ils` uses it, so the
  strategies act on a folder's first page while the next ones are fetched.
- `close()`, or collecting the file system, shuts the threads down.
- `_mkdir` and `_makedirs` check the preconditions, then call the subclass's `_create_directory`.
- `operation_concurrency` and `io_concurrency` default to 16 and 8.
- `open` writes text with `"\n"` newlines by default.

fsspec mirrors the async methods as sync ones, run on fsspec's event loop. `DatabricksFileSystem` calls those.

### Paths

`path.py` matches paths with three regular expressions: `dbfs:/` URLs, Workspace paths and Volume paths.

- `strip_scheme` and `unstrip_scheme` convert between `dbfs:/` URLs and POSIX paths.
- `fs_type` gives a path's backend.
- `parse_volume_path` gives the catalog, schema, volume and path in the volume. Missing parts are `None`.
- `parse_workspace_path` and `parse_dbfs_path` give the path in their API.
- `to_local_path` and `from_local_path` convert to and from local paths on Databricks compute.

### Errors

`error_mapping(path)` is a context manager around SDK calls. It maps the SDK's exceptions:

| SDK exception                            | Raised                          |
|------------------------------------------|---------------------------------|
| `NotFound`                               | `FileNotFoundError`             |
| `AlreadyExists`, `ResourceAlreadyExists` | `FileExistsError`               |
| `Unauthenticated`, `PermissionDenied`    | `PermissionError` with `EACCES` |

Other exceptions pass through. Helpers such as `file_not_found_error` make `OSError`s with the path and the system's
message.

### Volumes

`VolumeFileSystem`, in `volume.py`, implements the primitives, such as `_ils`, `_info`, `_rm_file`, `_rmdir`,
`_create_directory` and `_cp_file`. Its `io_concurrency` defaults to 24. Its `storage_profile` declares only
`fs_type="tree"`, so the strategies take the way that is right on any storage.

The APIs called depend on the depth of the path. `include_browse` is passed to the Unity Catalog APIs.

| Path                                   | `info`                                                    | `ls`                            |
|----------------------------------------|-----------------------------------------------------------|---------------------------------|
| `/Volumes`                             | None: a fixed folder                                      | `catalogs.list`                 |
| `/Volumes/<catalog>`                   | `catalogs.get`                                            | `schemas.list`                  |
| `/Volumes/<catalog>/<schema>`          | `schemas.get`                                             | `volumes.list`                  |
| `/Volumes/<catalog>/<schema>/<volume>` | `volumes.read`                                            | `files.list_directory_contents` |
| Deeper                                 | `files.get_metadata`, else `files.get_directory_metadata` | `files.list_directory_contents` |

`ls` of a file returns the file itself, from `files.get_metadata` after the listing fails.

Each metadata call is a round trip, so the checks inside a volume make as few as they can.
`DatabricksFileSystem` passes `isdir` and `isfile` to the backends. `test_volume_metadata_requests` counts them.

| Check                                        | Calls                                         |
|----------------------------------------------|-----------------------------------------------|
| `info` of a file, so also `size` and reading | 1: `files.get_metadata`                       |
| `info` of a directory                        | 2: `files.get_metadata`, then the directory's |
| `isdir`                                      | 1: `files.get_directory_metadata`             |
| `isfile`                                     | 1: `files.get_metadata`                       |
| Opening for writing                          | 1: `isdir`, to refuse directories             |
| `ls` of a directory                          | 1: `files.list_directory_contents`            |

Copies, moves and deletes inside a volume:

- `cp_file` streams the file from a readable Volume file into a writable one, as the Files API has no copy.
- `mv` is a copy, then a delete: the Files API has no rename.
- `rm` of a file calls `files.delete`. Only when that fails does it check for a directory.
- A recursive `rm`, `copy` or `mv` is a tree strategy, acting on each folder as soon as it is listed.
- Catalogs, schemas and volumes are never deleted or created as folders.

Two `asyncio.Semaphore`s bound the requests. `operation_concurrency` bounds those of the recursive operations, and
`io_concurrency` the range reads and part uploads of all the files opened. `test_volume_rm_requests` and
`test_volume_cp_file_streams` check them.

#### The HTTP session

`http_client.py` holds what the Volume and DBFS files share. Its `AioHttpSessionSupport` mixin gives each of the two file
systems one aiohttp `ClientSession`, with `connection_pool_size` connections:

- it is created on the loop that first uses it, as `TCPConnector` needs a running loop, and is never pickled;
- `close()` closes it on its loop;
- an aiohttp middleware authenticates requests with the SDK `Config`'s headers.

A failed request raises an `OSError`, chosen by its HTTP status or by the API's `error_code`.

#### Storage proxy or presigned URLs

On first open, the file system probes the Databricks storage proxy, `storage-proxy.databricks.com`, with the workspace
ID. If it answers, all requests go to it. Otherwise, requests go to the workspace's host, and data moves through
presigned URLs from the Files API.

Files smaller than `min_presigned_url_size`, 1 MB, are read from the Files API directly. Presigned URLs for reads are
renewed after 30 minutes.

#### Reading

`VolumeReadableFile` subclasses the library's `AdaptiveReadableFile`, and implements `_fetch_range` with a range `GET`.
The base class sizes the blocks, from 1 MB to 4 MB, and fetches ahead on sequential reads.

A range request that gets a `429`, `500`, `502`, `503` or `504` is retried up to 3 times, with exponential backoff and
jitter. Writes are not retried: a repeated `PUT` can fail with "already exists" when the first one went through.

#### Writing

`VolumeWritableFile` subclasses the library's `AdaptiveWritableFile`, which buffers the writes:

1. Data is buffered until it reaches `min_multipart_size`, 5 MB. A file closed before then is uploaded by one `PUT`.
2. Then the upload is initiated.
3. Parts are uploaded as the buffer fills, from 5 MB to 16 MB.
4. `close()` uploads the rest, waits for the parts, and completes the upload.

The server's answer to the initiation names the upload API, and the file builds a session object of it:

- `_MultipartSession` sends parts concurrently, then completes the upload with their ETags.
- `_ResumableSession` sends one part at a time, with `Content-Range`, so the file sets `sequential`. The last part
  completes the upload. A `308` answer gives the bytes received, and the rest is sent again.
- An answer naming neither API raises an `OSError`.

Multipart support follows the endpoint, not the cloud. The endpoints listing, `GET /api/2.0/fs/endpoints`, gives each
endpoint a `multipart_upload_supported` flag:

- the offload path on the workspace host, which presigned URLs use, supports multipart uploads;
- Files API 3.0 on the storage host supports them;
- Files API 2.0 called directly on the workspace host does not.

AWS and GCP workspaces answered the same, and both started multipart uploads in the live tests. The server's choice may
change with API versions, so the file follows the server's answer per upload.

If a part fails, `close()` aborts the upload and raises an `OSError`:

- A multipart upload is aborted by a `POST` to the Files API, as the SDK aborts. No abort URL is asked for, as Azure
  Databricks refuses them with a 400 answer.
- A resumable upload is aborted by a `DELETE` of its upload URL.
- The abort is best effort. A failed abort is a WARNING, and the upload's own failure is raised.

Part sizes must be multiples of 256 KB. The constructor refuses other minimum and maximum sizes.

### Workspace files

`WorkspaceFileSystem`, in `workspace.py`, uses the SDK's `WorkspaceClient.workspace`. Its `storage_profile` declares
`implicit_folders=False` and `renames_files=False`: directories are objects, and the API has no move.

`WorkspaceFile` is a sync `io.BytesIO` calling the SDK directly, with no event loop:

- Opening for reading or appending downloads the whole file into memory.
- Closing uploads it whole, if changed. Leaving a `with` block by an exception uploads nothing.
- `open_async()` opens it in a thread, and wraps it in the library's `SyncFileAdapter`.
- Exclusive creation uploads without overwriting, so that the API refuses a file created meanwhile.
- Files are at most 10 MB.

Notebooks are objects of type `NOTEBOOK`, and `notebook_format` gives their export and import format, `SOURCE` by
default. An existing notebook keeps its language, and a new one needs `language`. Other files use `AUTO` export and
`RAW` import, and refuse `notebook_format`.

- `cp_file` downloads and uploads, keeping notebooks as notebooks, and overwrites the destination.
- `_rm_tree` is the API's recursive delete, which a recursive `rm` uses on each directory it is given.
- `rm` of a file checks its type first, as the API deletes an empty directory as a file.

### Legacy DBFS

`DBFS`, in `dbfs.py`, uses the SDK's `WorkspaceClient.dbfs`.

- `_mv_file` is the API's move, which moves a directory with all in it. So `storage_profile` declares
  `renames_folders=True`, and a recursive `mv` moves each folder whole where it can.
- `_rm_tree` is the API's recursive delete, one request per directory a recursive `rm` is given.
- `rmdir` refuses a directory that is not empty. It calls the API's non-recursive delete, which the SDK's
  `dbfs.delete` refuses for directories.
- Files are the library's adaptive files, `DbfsReadableFile` and `DbfsWritableFile`. They call the API with the file
  system's aiohttp session, as Volume files do, and share its `io_limit`.
- The SDK's own `dbfs.open` cannot seek, and reads one block at a time.
- Reads are retried after transient errors, as Volume reads are. Writes are not retried, as a repeated add-block would
  append its data twice.
- The API's read, add-block and put each take 1 MB at most. The SDK sends 1 MiB of raw bytes per add-block, so the
  limit is taken to be on the decoded data.
- A read fetches ranges of 1 MB at most, several at once. The API names no version of a file, so a file replaced while
  read is not detected.
- A file of 1 MB at most is written by one put. A larger one is a stream: create, add-blocks in order, close. So the
  writer is `sequential`.
- Create replaces the file there at once, and the file grows with each block. An aborted stream is closed, then its
  file deleted: a failed `wb` write over a file leaves no file.
- Outside a transaction, a file writes to its own path. A temporary path would need a delete and a move on commit, as
  the API's move refuses an existing destination, and a crash would leave the temporary file.
- Mode `x` is checked when opening. Create and put check it again, with overwrite off.
- Append mode is refused, as the API's create cannot append.

### File objects

Every backend's files are the library's file objects. They raise `RuntimeError` when their sync methods are called
from the event loop. Code on the loop uses `open_async()` and awaits the file's methods.

### Transactions

fsspec opens a file with `autocommit=False` inside `with fs.transaction:`. Each backend's `_open` then returns the
library's `TransactionFile`, with a `FileTransaction` of `files/fsspec_transaction.py`:

- Volumes use `AdaptiveWritableTransaction`, with `hold_last_part=True`. A resumable upload completes with its last
  part, and the server chooses that API only once the upload starts.
- Workspace files use `_WorkspaceTransaction`, in `workspace.py`. Commit closes the `WorkspaceFile`, which uploads it,
  and discard drops it.
- DBFS uses `TempPathTransaction`. Its stream shows the file as it grows, so holding a last part would not hide it.
  Commit deletes the destination, then moves the temporary file, as the API's move refuses an existing destination.

The router's `_open` passes `autocommit` to the backend's `_open`. fsspec's `open` adds the file to the router's own
transaction, which commits or discards the files of every backend. The backends' own transactions stay empty.

The router's `_pipe_file` writes through `open_async`, which knows no transaction. So `DatabricksFileSystem.pipe_file`
writes with `open` inside a transaction.

### Local file system

`DatabricksLocalFileSystem`, in `local.py`, is fsspec's `LocalFileSystem` with the `dbfs` protocol. It gets local
paths from `DatabricksFileSystem`. `path.to_local_path` gives a path's local path on Databricks compute:

- Volume and Workspace paths are the same.
- Legacy DBFS paths are under `legacy_dbfs_local_root`, `/dbfs` by default.

`path.from_local_path` is the reverse, for the names that `ls` and `info` return.

### Logging

Logging follows s3fs and gcsfs. Each module has a logger of its name, under `fsspec_databricks`, and no handlers are
installed.

- Each request to Databricks is one DEBUG line: its API or HTTP method, the path and the outcome.
  `BaseDatabricksFileSystem._run_in_thread` logs the SDK's calls. `check_response` in `http_client.py` logs the
  files' HTTP requests, and `volume.py` its storage proxy probe.
- Range reads are not logged, as a read makes many. Their retries are still logged.
- Retries and aborted uploads are WARNINGs.
- The fallback from the storage proxy is DEBUG: outside Databricks, the proxy is normally absent.
- Operations, such as `find` or `rm`, are not logged here. The library's monitors report them.

### The copy of fsspec-alt-strategies

`src/fsspec_databricks/_contrib/strategy/` is a copy of the package `fsspec-alt-strategies-impl`, version 0.1.0, from
[fsspec-alt-strategies](https://github.com/sugibuchi/fsspec-alt-strategies). It is copied whole, as its `wrapper.py`
needs `flat/` too. Its modules keep their license headers and relative imports, and ruff skips them.

Their shared interface is not copied. It is the dependency `fsspec-alt-strategies>=0.1,<0.2`, from PyPI.

The bounds stop at the copy's minor version. Before 1.0, a minor release of the interface may break the copy, and the
copy never changes in an installed release.

To refresh the copy:

1. Copy the package over the old copy with its command line, which replaces it whole:

   ```bash
   uv run --with "fsspec-alt-strategies-impl[cli]==<version>" fsspec-alt-strategies-impl copy src/fsspec_databricks/_contrib/strategy
   ```

2. Update the version above, and the interface's bounds in `pyproject.toml` to the copy's minor version, then run
   `uv lock`.
3. Run the tests.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All Python goes through uv: never call `python` or `pip` directly.

```bash
uv sync                                         # the package, editable, with the dev dependencies (uv.lock)
uv lock                                         # update uv.lock after changing dependencies, and commit it
uv run pytest                                   # all tests, on the dummy server: no workspace needed
uv run pytest --api=live tests/test_dbfs.py     # the backend tests on the live workspace; --api=both for both
uv run pytest --fs=volume,workspace             # only these backends: volume, workspace, dbfs
uv run pytest tests/test_path.py -k volume      # one file / matching tests
uv run pytest --cov                             # with coverage of src/
uvx ruff check .                                # lint (ruff is not a dev dependency; config in pyproject.toml)
uvx ruff format .                               # format
uvx pre-commit run --all-files                  # the hooks: ruff, whitespace, end of files, YAML
```

CI, in `.github/workflows/ci.yaml`, runs on pull requests and pushes to `main`:

- `lint`: ruff check, ruff format check, zizmor and actionlint;
- `test`: pytest on the dummy server on Python 3.11 to 3.14, with coverage to Codecov from 3.14;
- `live`: pytest `--api=live` on Python 3.14, on the `aws`, `azure` and `gcp` environments, never for forks.

`codeql.yaml` scans the code and workflows. `release.yaml` releases a pushed `v*` tag: DEVELOPMENT.md describes it.

The live jobs hold Databricks credentials. Keep their supply chain fixed:

- `uv.lock` is committed, and CI installs with `uv sync --locked`. After changing dependencies, run `uv lock` and
  commit `uv.lock`.
- Pin every action to a full commit SHA, with its version in a comment, e.g. `@<sha>  # v7.0.1`. Dependabot updates
  them. Also pin uv's `version`.
- Check out with `persist-credentials: false`: no job pushes.
- Never add a `pull_request_target` trigger, or give secrets to `pull_request` runs from forks.
- Each workflow declares `permissions:`, with only what it needs.
- Check changes with `uvx zizmor --offline .github/workflows` and `uvx --from actionlint-py actionlint`.

## Tests and layout

DEVELOPMENT.md describes the layout, the tests and their setup. Read its Tests section before writing tests. The traps:

- `uv run pytest` runs everything on the dummy server, `tests/dummy_api.py`, with no credentials. `--api=live` or
  `--api=both` runs the backend tests on the workspace too, from `.env` or the environment.
- Without `--api` and `--fs`, `FSSPEC_DATABRICKS_TEST_API` and `FSSPEC_DATABRICKS_TEST_FS` choose, from the
  environment or `.env`.
- `--fs` selects the backends. Each backend test has a `backend` marker, and a test spanning backends names them all.
- Run live files one at a time: the Volumes tests take about 20 minutes.
- The session fixtures `volume_test_root`, `workspace_test_root` and `dbfs_test_root` delete everything in the test
  roots first. Never point them at folders holding anything else.
- Never fake the SDK or its clients. A backend test takes `client`, which is the dummy's or the workspace's.
- A test taking the `dummy` fixture runs on the dummy only. Count requests with `dummy.requests`, never with mocks.
- `tests/dummy_api.py` serves the Files, Workspace, DBFS and Unity Catalog APIs over HTTP, to the SDK's real
  `WorkspaceClient`. Follow the REST reference there, and mark behaviour it leaves open as an assumption.
- One test file per module. Test helpers shared by files are in `tests/utils.py`: test files never import each other.
- `tests/conftest.py` sets `cachable = False`, so each test gets new file system instances.

## Architecture

DEVELOPMENT.md's Architecture section describes each part: read it before changing one. The traps:

- `DatabricksFileSystem` is the library's `AsyncRoutingFileSystem`. It implements `_route`, `_outer` and `_roots`.
  Override a method that the router passes on only for what Databricks adds, e.g. `open`'s notebook parameters.
- `_route(path)` gives the backend and its path. Inside a workspace, that path is a local path, and `_outer` maps the
  names returned back with `from_local_path`.
- Every backend must be async. The local file system is wrapped in the library's `SyncTreeWrapper`.
- Every Databricks API backend is an async `TreeFileSystem`. Volumes, Workspace and DBFS subclass `BaseDatabricksFileSystem` in `base.py`,
  and call the SDK in threads with `_run_in_thread`, passing the SDK's bound method and its arguments.
- Wrap Databricks SDK calls in `error_mapping(path)`, so that callers get `OSError` subclasses.
- Parse paths with the helpers of `path.py`. Every method takes `dbfs:/` URLs and POSIX paths alike.
- `VolumeFileSystem` and `DBFS` are async fsspec file systems on fsspec's loop. Their files call the REST APIs with
  aiohttp, through the helpers of `http_client.py`.
- Their aiohttp session is created on first use, as `TCPConnector` needs a running loop. A file system runs on one
  loop, as fsspec caches it: never check the running loop, nor make a client again for another.
- Never block on the loop from code running in it. File objects' synchronous methods raise there.
- `src/fsspec_databricks/_contrib/strategy/` is a copy of fsspec-alt-strategies. Never edit it: DEVELOPMENT.md says how
  to refresh it.
- `DatabricksFileSystem.__init__` takes every parameter of the SDK's `WorkspaceClient`, in its order.
  `test_dbx_fs_accepts_workspace_client_params` fails when the SDK adds one: add it to the signature, the docstring
  and `config_params`.
- File systems must stay picklable. fsspec pickles them as their constructor arguments, so every argument must be
  picklable. Runtime attributes are never pickled.
- Multipart upload, the endpoints list and Files API 3.0 are missing from the REST reference. The Databricks SDK for
  Python's `mixins/files.py` uses them: follow it. Volume files speak no resumable upload, as the SDK since 0.163.
- Workspace files are `WorkspaceFile`s, sync and buffered in memory, transferred whole, up to the API's 10 MB.
- Legacy DBFS `rmdir` never deletes recursively. `tests/test_dbfs.py` checks it on the dummy server.
- Logging follows s3fs and gcsfs. Each module has `logging.getLogger(__name__)`, and the library installs no handlers.
- Log one DEBUG line per request to Databricks, where it leaves the process: `_run_in_thread` in `base.py`, and
  `check_response` in `http_client.py`. Range reads are not logged. Keep it cheap, with lazy `%` formatting.
- Log retries and aborted uploads at WARNING. Log nothing else: the library's monitors report operations.

## Conventions

### Code

- Comments are short, and say why the code does something not obvious. Keep that density and tone.
- Type hints use built-in generics and `X | None`.
- Each module ends with `__all__`.

### Docstrings

Docstrings of the public API are in numpy style: `Parameters`, `Returns`, `Raises`. The public API is what users
construct or call:

- `DatabricksFileSystem` and `use`;
- `VolumeFileSystem`, `WorkspaceFileSystem` and `DBFS`, with their constructors and `open`.

Internal docstrings stay one line or short prose: file objects, helpers and underscore names.

### Writing docs

Write docs so that people can review them. That covers this file, DEVELOPMENT.md and the user docs.

- One idea per sentence: about 30 words, never more than 50.
- At most one parenthesis per sentence, never nested.
- Three or more items make a list.
- Add a new fact as its own sentence or bullet where it belongs. Never append it to an existing sentence.
- Name an identifier only when the reader needs it to find the code. A function's details belong in its docstring.
- Describe what is, not what was: git keeps the history.
- When editing a passage that breaks these rules, rewrite it.

### Where docs go

- README.md is the quick start: features, installation, paths, compatibility, status and limitations.
- USAGE.md has the user-facing details: operations, each backend's behaviour, and the options.
- AUTHENTICATION.md covers the ways to configure authentication.
- DEVELOPMENT.md has setup, tests, CI, releasing, the design choices and the architecture.
- SECURITY.md says how to report vulnerabilities, and what is in scope.

Update the docs when behaviour, parameters or options change. Keep user docs short and compact: what users do, not how
it works or why.

After each release, rewrite its GitHub release notes as DEVELOPMENT.md's "Release notes" says, without being asked.

### Links

README.md, USAGE.md and AUTHENTICATION.md are user docs. README.md uses absolute GitHub URLs for links to files, as
PyPI shows it but cannot resolve relative paths. The other docs are read on GitHub only, so they use relative paths.

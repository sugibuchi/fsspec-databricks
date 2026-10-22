# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All Python goes through uv: never call `python` or `pip` directly.

```bash
uv sync                                         # the package, editable, with the dev dependencies (uv.lock)
uv lock                                         # update uv.lock after changing dependencies, and commit it
uv run pytest                                   # all tests: most need a Databricks workspace (see below)
uv run pytest tests/test_path.py -k volume      # one file / matching tests
uv run pytest --cov                             # with coverage of src/
uvx ruff check .                                # lint (ruff is not a dev dependency; config in pyproject.toml)
uvx ruff format .                               # format
uvx pre-commit run --all-files                  # the hooks: ruff, whitespace, end of files, YAML
```

CI, in `.github/workflows/on_push.yaml`, runs on pushes:

- ruff check and ruff format check, with the latest ruff;
- pytest on Python 3.14 against the default workspace, with coverage on `main`;
- on `main`, pytest on the other supported Python versions;
- pytest on Azure, AWS or GCP workspaces named in the `TEST_ENVIRONMENTS` variable.

Releases are published GitHub releases. DEVELOPMENT.md describes them.

The test jobs hold Databricks credentials. Keep their supply chain fixed:

- `uv.lock` is committed, and CI installs with `uv sync --locked`. After changing dependencies, run `uv lock` and
  commit `uv.lock`.
- Actions are pinned to commit SHAs, with the version in a comment, e.g. `@<sha> # v7.0.1`. Dependabot updates both.
- `astral-sh/setup-uv` publishes no major-version tags: pin a full version's SHA.
- Never add a `pull_request_target` trigger, or give secrets to `pull_request` runs from forks.
- Each workflow declares `permissions:`, with only what it needs.

## Tests and layout

DEVELOPMENT.md describes the layout, the tests and their setup. Read its Tests section before writing tests. The traps:

- Most tests run against a real workspace. They need credentials and the two test root variables, from `.env` or the
  environment.
- The session fixtures `volume_test_root` and `workspace_test_root` delete everything in the test roots first. Never
  point them at folders holding anything else.
- Without credentials, run `tests/test_file.py`, `tests/test_volume.py`, `tests/test_path.py` and `tests/test_utils.py`.
  `test_volume.py` and `test_file.py` use the fake Files API in `tests/dummy_api.py`.
- A test of anything a workspace serves gets a check without one where possible, on the fake API or with mocks.
- Test helpers shared by files are in `tests/utils.py`.
- `tests/conftest.py` sets `cachable = False`, so each test gets new file system instances.

## Architecture

DEVELOPMENT.md's Architecture section describes each part: read it before changing one. The traps:

- `DatabricksFileSystem` routes each call to a backend by path. It holds no file system logic of its own, except for
  the root folder and transfers between backends.
- Its methods get the backend and the path to pass with `_resolve(path)`. Inside a workspace, that path is a local
  path, and names returned are mapped back with `from_local_path`.
- Base class methods in `base.py` only check preconditions. Subclasses call `super()` first, then the API.
- Wrap Databricks SDK calls in `error_mapping(path)`, so that callers get `OSError` subclasses.
- Parse paths with the helpers of `path.py`. Every method takes `dbfs:/` URLs and POSIX paths alike.
- aiohttp objects are created and used in the Volume backend's IO event loop. `TCPConnector` needs a running loop.
- Never block on the IO loop from code running in it. File objects' synchronous methods raise there.
- `DatabricksFileSystem.__init__` takes every parameter of the SDK's `WorkspaceClient`, in its order.
  `test_dbx_fs_accepts_workspace_client_params` fails when the SDK adds one: add it to the signature, the docstring
  and `config_params`.
- File systems must stay picklable. A new attribute holding a client, a loop, a thread or a session is dropped in
  `__getstate__` and reset in `__setstate__`.
- Multipart upload uses endpoints missing from the REST reference. The Databricks SDK for Python's
  `mixins/files.py` uses the same: follow it. Keep the fallback to resumable upload working.
- Volume write block sizes are multiples of 256 KB.
- Workspace files are cached whole in memory, up to the API's 10 MB.
- Chatty debug logs go under `if self.verbose_debug_log:`.

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

Update the docs when behaviour, parameters or options change. Keep user docs short and compact: what users do, not how
it works or why.

### Links

README.md, USAGE.md and AUTHENTICATION.md are user docs. README.md uses absolute GitHub URLs for links to files, as
PyPI shows it but cannot resolve relative paths. The other docs are read on GitHub only, so they use relative paths.

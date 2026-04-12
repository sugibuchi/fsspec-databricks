# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests (requires Databricks workspace access)
uv run pytest

# Run a single test file
uv run pytest tests/test_volume_fs.py

# Run a single test by name
uv run pytest tests/test_volume_fs.py::test_function_name -k "test_name"

# Run tests without real Databricks (dummy API only)
uv run pytest tests/test_file.py

# Lint
uv run ruff check src/ tests/

# Format check
uv run ruff format --check src/ tests/
```

Tests load a `.env` file from the project root automatically via `python-dotenv`. Set `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT`, and `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` there for local development.

## Architecture

### Path routing

`DatabricksFileSystem` (`spec.py`) is the public entry point. It parses every path via `path.py` and delegates to one of three backends:

| Path pattern | Backend class |
|---|---|
| `/Volumes/(catalog)/(schema)/(volume)/...` | `VolumeFileSystem` (`volume.py`) |
| `/Workspace/...` | `WorkspaceFileSystem` (`workspace.py`) |
| Anything else (legacy) | `DBFS` (`dbfs.py`) |

When running inside a Databricks workspace, operations can be transparently redirected to the local filesystem (`local.py`) via `use_local_fs_in_workspace=True`.

### File I/O layer (`file.py`)

All file objects inherit from `AbstractFile → AbstractAsyncFile → FileRangeTaskSupport → AbstractAsyncReadable/WritableFile`. The concrete implementations (`VolumeReadableFile`, `VolumeWritableFile`) live in `volume.py`.

Key design points:
- Async operations run in a **background thread with a dedicated event loop**, bridged to synchronous callers via `_run_and_wait()`.
- Reads are **concurrent and prefetching**: block size adapts upward when `block_size * max_concurrency < read_length`.
- Writes buffer into `BytesIO` and switch to **multipart or resumable upload** once the buffer exceeds `volume_min_multipart_upload_size` (default 5 MB). The multipart upload API used is undocumented/unofficial.
- A semaphore (`FileRangeTaskSupport`) caps concurrent in-flight requests.
- The bare `except:` around `self._semaphore.release()` in `file.py` is intentional — it must always release regardless of any exception.

### Error mapping (`error.py`)

All API calls are wrapped with `with error_mapping(path):`, which converts Databricks SDK exceptions (`NotFound`, `AlreadyExists`, `PermissionDenied`, etc.) into standard Python OSError subclasses.

### Base class (`base.py`)

`AbstractDatabricksFileSystem` extends fsspec's `AbstractFileSystem` and implements shared operations (stat, ls, mkdir, rm, copy) using the Databricks SDK `WorkspaceClient`. Subclasses override the abstract methods for their specific API.

## Key implementation notes

- `_tasks` is **not** cleared after `gather()` in `_upload_buffered_data(flush=True)` — the task tokens are needed later by `_do_complete_multipart_upload()`.
- The 200/201 status check on non-final parts in `_upload_resumable_part` is not reachable in practice; the API contract guarantees the server never returns 200/201 unless `last_part=True`.
- String-based error message matching (`"is not empty" in e.args[0]`) in `workspace.py` and `volume.py` is a known fragility when checking for non-empty directory errors.

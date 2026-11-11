"""Fixtures and options of the tests.

`--api` chooses where the backend tests run: on the dummy server, on the live workspace, or both. `--fs` chooses the
backends tested. Without them, `FSSPEC_DATABRICKS_TEST_API` and `FSSPEC_DATABRICKS_TEST_FS` choose, from the
environment or `.env`. A test of a backend takes `client`, or a fixture built on it, and runs once per API chosen.
"""

import asyncio
import logging
import os
import tempfile
from asyncio import AbstractEventLoop
from base64 import b64encode
from random import randbytes, randint
from threading import Event, Thread

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from aiohttp import ClientSession
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from dummy_api import dummy_storage, serve_app
from fsspec.asyn import get_loop, sync
from utils import (
    legacy_dbfs_disabled,
    prepare_dbfs_test_root,
    prepare_volume_test_root,
    prepare_workspace_test_root,
)

from fsspec_databricks.base import BaseDatabricksFileSystem
from fsspec_databricks.spec import DatabricksFileSystem
from fsspec_databricks.volume import VolumeFileSystem

log = logging.getLogger(__name__)

_apis = {"dummy": ["dummy"], "live": ["live"], "both": ["dummy", "live"]}
_backends = ("volume", "workspace", "dbfs")
_no_config_file = os.devnull + ".none"


def pytest_addoption(parser):
    # Without the options, the environment chooses, .env included: read when used, as .env is loaded after this.
    parser.addoption(
        "--api",
        choices=sorted(_apis),
        default=None,
        help="Where the backend tests run: the dummy server, the live workspace, or both. "
        "Default: $FSSPEC_DATABRICKS_TEST_API, else dummy.",
    )
    parser.addoption(
        "--fs",
        action="append",
        default=None,
        help="The backends tested, comma-separated or repeated: volume, workspace, dbfs. "
        "Default: $FSSPEC_DATABRICKS_TEST_FS, else all.",
    )


def pytest_configure(config):
    load_dotenv()
    logging.getLogger("databricks.sdk").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    BaseDatabricksFileSystem.cachable = False
    VolumeFileSystem.cachable = False
    DatabricksFileSystem.cachable = False
    config.addinivalue_line("markers", "dummy_only: runs on the dummy server only")
    config.addinivalue_line("markers", "live_only: runs on the live workspace only")
    config.addinivalue_line(
        "markers", "backend(*names): the backends a test needs: volume, workspace, dbfs"
    )


def _selected_api(config) -> str:
    chosen = config.getoption("--api") or os.environ.get(
        "FSSPEC_DATABRICKS_TEST_API", "dummy"
    )
    if chosen not in _apis:
        raise pytest.UsageError(
            f"Unknown FSSPEC_DATABRICKS_TEST_API: {chosen}, not one of {', '.join(sorted(_apis))}"
        )
    return chosen


def _selected_backends(config) -> set[str]:
    values = config.getoption("--fs") or [
        os.environ.get("FSSPEC_DATABRICKS_TEST_FS", "")
    ]
    names = {n.strip() for v in values for n in v.split(",")}
    names.discard("")
    unknown = names - set(_backends)
    if unknown:
        raise pytest.UsageError(
            f"Unknown --fs or FSSPEC_DATABRICKS_TEST_FS backends: {', '.join(sorted(unknown))}"
        )
    return names or set(_backends)


def pytest_generate_tests(metafunc):
    """Parametrize each backend test by its APIs, and tag it with its backends, both shown in its id."""
    names = [n for m in metafunc.definition.iter_markers("backend") for n in m.args]
    if names:
        metafunc.parametrize("backend", ["+".join(names)], indirect=True)

    if "api" not in metafunc.fixturenames:
        return
    chosen = _selected_api(metafunc.config)
    only = None
    if metafunc.definition.get_closest_marker("live_only"):
        only = "live"
    if metafunc.definition.get_closest_marker("dummy_only") or (
        "dummy" in metafunc.fixturenames
    ):
        only = "dummy"
    if only and only not in _apis[chosen]:
        reason = f"{only} only, not --api={chosen}"
        values = [pytest.param(only, marks=pytest.mark.skip(reason=reason))]
    else:
        values = [only] if only else _apis[chosen]
    metafunc.parametrize("api", values, scope="session")


def pytest_collection_modifyitems(config, items):
    selected = _selected_backends(config)
    keep, drop = [], []
    for item in items:
        names = {n for m in item.iter_markers("backend") for n in m.args}
        (drop if names - selected else keep).append(item)
    if drop:
        config.hook.pytest_deselected(items=drop)
        items[:] = keep


@pytest.fixture(autouse=True)
def backend(request):
    """The backends of the test, from its `backend` marker, so that its id names them."""
    return getattr(request, "param", None)


@pytest.fixture(autouse=True)
def _api_environment(request, monkeypatch):
    """Point default clients at the dummy server, unless the test runs live, so that no test needs credentials."""
    if "api" in request.fixturenames and request.getfixturevalue("api") == "live":
        return
    url = request.getfixturevalue("dummy_api")
    for name in list(os.environ):
        if name.startswith("DATABRICKS_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("DATABRICKS_HOST", url)
    monkeypatch.setenv("DATABRICKS_TOKEN", "dummy")
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "pat")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", _no_config_file)


def _event_loop(thread_name: str):
    loop: AbstractEventLoop | None = None

    ready = Event()

    def run():
        nonlocal loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    thread = Thread(name=thread_name, target=run, daemon=True)
    thread.start()
    ready.wait(timeout=5.0)

    if not loop or not loop.is_running():
        raise RuntimeError(f"Event loop in thread {thread_name} failed to start")

    try:
        yield loop
    finally:
        if loop and not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5.0)
        if thread.is_alive():
            raise RuntimeError(f"Event loop thread {thread_name} failed to stop")


@pytest.fixture(scope="session")
def server_event_loop():
    yield from _event_loop("server-event-loop")


@pytest.fixture(scope="session")
def client_event_loop():
    """fsspec's event loop, which the file systems and their files run on."""
    return get_loop()


@pytest.fixture(scope="session")
def dummy_api(server_event_loop):
    """The URL of the dummy server."""
    yield from serve_app(server_event_loop)


@pytest.fixture(scope="session")
def dummy_client(dummy_api):
    """A `WorkspaceClient` of the dummy server."""
    return WorkspaceClient(
        host=dummy_api, token="dummy", auth_type="pat", config_file=_no_config_file
    )


@pytest.fixture
def dummy(api, dummy_api):
    """The dummy server's state, with its switches and the requests it served, reset after the test.

    A test taking it runs on the dummy server only.
    """
    storage = dummy_storage()
    storage.requests.clear()
    try:
        yield storage
    finally:
        storage.reset()


@pytest.fixture(scope="session")
def live_client():
    """A `WorkspaceClient` of the live workspace.

    Live tests are opt in, with `--api` or `FSSPEC_DATABRICKS_TEST_API`. Once asked for, a workspace that cannot be
    reached is an error, not a skip that would pass unseen.
    """
    # The SDK asks for the scope all-apis unless given others, and has no environment variable for them. A secret
    # limited to some scopes refuses all-apis: FSSPEC_DATABRICKS_TEST_SCOPES names them, comma or space separated.
    scopes = os.environ.get("FSSPEC_DATABRICKS_TEST_SCOPES") or None
    try:
        client = WorkspaceClient(scopes=scopes)
        client.current_user.me()
    except (
        Exception
    ) as e:  # The SDK raises ValueError, among others, without a configuration.
        pytest.fail(f"No live Databricks workspace: {e}", pytrace=False)
    return client


@pytest.fixture(scope="session")
def client(api, request) -> WorkspaceClient:
    """The client of the API the test runs on: the dummy server's, or the live workspace's."""
    return request.getfixturevalue(f"{api}_client")


@pytest.fixture(scope="session")
def volume_test_root(client):
    return prepare_volume_test_root(client)


@pytest.fixture(scope="session")
def workspace_test_root(client):
    return prepare_workspace_test_root(client)


@pytest.fixture(scope="session")
def dbfs_client(client, api) -> WorkspaceClient:
    """The client, or a skip when the workspace disables legacy DBFS."""
    if legacy_dbfs_disabled(client):
        pytest.skip(f"The {api} workspace disables legacy DBFS")
    return client


@pytest.fixture(scope="session")
def dbfs_test_root(dbfs_client):
    return prepare_dbfs_test_root(dbfs_client)


@pytest.fixture
def aiohttp_session(dummy_api, client_event_loop):
    """A function-scoped aiohttp ClientSession on fsspec's loop, for the dummy server."""

    async def _create():
        return ClientSession()

    session = sync(client_event_loop, _create)
    try:
        yield session
    finally:
        sync(client_event_loop, session.close)


@pytest.fixture(scope="session")
def dummy_table():
    return [
        {
            "id": i,
            "col_1": randint(0, 256),
            "col_2": b64encode(randbytes(64)),
        }
        for i in range(256 * 1024)
    ]


@pytest.fixture(scope="session")
def dummy_table_parquet(dummy_table):
    with tempfile.TemporaryFile() as f:
        pq.write_table(pa.Table.from_pylist(dummy_table), f)
        f.seek(0)
        data = f.read()

    return data

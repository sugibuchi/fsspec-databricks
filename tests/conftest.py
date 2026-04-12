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
from databricks.sdk.errors import NotFound
from dotenv import load_dotenv

from fsspec_databricks.base import AbstractDatabricksFileSystem
from fsspec_databricks.path import parse_volume_path

from .dummy_api import dummy_context, serve_app

log = logging.getLogger(__name__)


def pytest_configure(config):
    load_dotenv()
    logging.getLogger("databricks.sdk").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    AbstractDatabricksFileSystem.cachable = False


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
    log.debug("Starting thread for event loop: %s", thread_name)
    thread.start()
    ready.wait(timeout=5.0)

    if not loop or not loop.is_running():
        raise RuntimeError(f"Event loop in thread {thread_name} failed to start")

    log.debug("Event loop in thread %s is ready: %s", thread_name, loop)

    try:
        yield loop
    finally:
        if loop and not loop.is_closed():
            log.debug("Stopping event loop in thread: %s", thread_name)
            loop.call_soon_threadsafe(loop.stop)
        log.debug("Waiting for event loop thread %s to stop", thread_name)
        thread.join(timeout=5.0)
        if thread.is_alive():
            raise RuntimeError(f"Event loop thread {thread_name} failed to stop")
        else:
            log.debug("Event loop thread %s has stopped", thread_name)


@pytest.fixture(scope="session")
def server_event_loop():
    yield from _event_loop("server-event-loop")


@pytest.fixture(scope="function")
def client_event_loop():
    yield from _event_loop("client-event-loop")


@pytest.fixture(scope="session")
def dummy_api(server_event_loop):
    yield from serve_app(server_event_loop)


@pytest.fixture(scope="function")
def aiohttp_session(dummy_api, client_event_loop):
    """A function-scoped aiohttp ClientSession connected to the dummy API server."""

    async def _create():
        return ClientSession()

    session = asyncio.run_coroutine_threadsafe(_create(), client_event_loop).result()
    try:
        yield session
    finally:
        asyncio.run_coroutine_threadsafe(session.close(), client_event_loop).result()


@pytest.fixture(scope="function")
def dummy_api_context(dummy_api):
    context = dummy_context()
    try:
        yield context
    finally:
        context.files.clear()
        context.multipart_upload_sessions.clear()
        context.resumable_upload_sessions.clear()

        context.check_signed_url = False
        context.upload_mode = "multipart"


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


@pytest.fixture(scope="session")
def client():
    return WorkspaceClient()


default_volume_test_root = "/Volumes/fsspec_databricks_test/fsspec_dbx_test/test/local"


@pytest.fixture(scope="session")
def volume_test_root(client):
    path = os.getenv(
        "FSSPEC_DATABRICKS_VOLUME_TEST_ROOT",
        default_volume_test_root,
    )

    path = "/" + path.strip("/")

    # Changed f-string logging to lazy-format
    log.info("Preparing volume test root: %s", path)

    catalog, schema, volume, _, fullpath = parse_volume_path(path)

    # Check existing of the UC volume for tests
    client.volumes.read(f"{catalog}.{schema}.{volume}")

    try:
        client.files.get_directory_metadata(fullpath)
    except NotFound:
        # Directory does not exist, create it
        client.files.create_directory(fullpath)
        # Changed f-string logging to lazy-format
        log.info("Volume test root has been created: %s", path)
        return path

    for i in client.files.list_directory_contents(directory_path=fullpath):
        try:
            client.dbfs.delete(i.path, recursive=True)
        except NotFound:  # Strange but NotFound sometimes thrown on Databricks on GCP  # noqa: PERF203
            if i.is_directory:
                client.files.delete_directory(i.path)
            else:
                client.files.delete(i.path)

    # Changed f-string logging to lazy-format
    log.info("Volume test root has been cleaned: %s", path)

    return path


default_workspace_test_root = "/fsspec-databricks-test/local"


@pytest.fixture(scope="session")
def workspace_test_root(client):
    path = os.getenv(
        "FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT",
        default_workspace_test_root,
    )

    path = "/" + path.strip("/")

    # Changed f-string logging to lazy-format
    log.info("Preparing workspace test root: %s", path)

    try:
        client.workspace.get_status(path)
    except NotFound:
        # Directory does not exist, create it
        client.workspace.mkdirs(path)
        # Changed f-string logging to lazy-format
        log.info("Workspace test root has been created: %s", path)
        return path

    for i in client.workspace.list(path):
        client.workspace.delete(i.path, recursive=True)

    # Changed f-string logging to lazy-format
    log.info("Workspace test root has been cleaned: %s", path)

    return path

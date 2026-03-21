import asyncio
import logging
import os
from asyncio import AbstractEventLoop
from threading import Event, Thread

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from dotenv import load_dotenv

from fsspec_databricks.base import AbstractDatabricksFileSystem
from fsspec_databricks.path import parse_volume_path

from .dummy_api import dummy_context, serve_app

load_dotenv()

logging.getLogger("databricks.sdk").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)

log = logging.getLogger(__name__)

# Disabling the caching of file system instances for Databricks
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
            if loop and not loop.is_closed():
                loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    thread = Thread(name=thread_name, target=run)
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


@pytest.fixture(scope="function")
def client_event_loop():
    yield from _event_loop("client-event-loop")


@pytest.fixture(scope="session")
def dummy_api(server_event_loop):
    yield from serve_app(server_event_loop)


@pytest.fixture(scope="function")
def dummy_api_context(dummy_api):
    context = dummy_context()
    try:
        yield context
    finally:
        context.files.clear()
        context.upload_sessions.clear()


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
        client.dbfs.delete(i.path, recursive=True)

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

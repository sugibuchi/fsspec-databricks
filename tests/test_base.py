"""Tests of `BaseDatabricksFileSystem`: its client, its threads, closing and pickling."""

import asyncio
import pickle
import threading
import time

import pytest
from databricks.sdk import WorkspaceClient
from fsspec_alt_strategies import StorageProfile

from fsspec_databricks.base import BaseDatabricksFileSystem


class DummyTreeFileSystem(BaseDatabricksFileSystem):
    # The client and the threads need none of the primitives.
    storage_profile = StorageProfile()
    _cp_file = _ils = _info = _open = _rm_file = _rmdir = open_async = None


def test_tree_fs_client(dummy_client: WorkspaceClient):
    client = dummy_client
    fs = DummyTreeFileSystem(config_params={"a": "b"})

    assert fs.config is not None
    assert fs.client is not None
    assert fs._config is None
    assert fs._client is None
    assert fs._config_params == {"a": "b"}

    fs = DummyTreeFileSystem(config=client.config)
    assert fs.config is client.config
    assert fs.client is not None
    assert fs.client is not client
    assert fs._config is client.config
    assert fs._client is None
    assert fs._config_params == {}

    fs = DummyTreeFileSystem(client=client)
    assert fs.config is client.config
    assert fs.client is client
    assert fs._config is None
    assert fs._client is client
    assert fs._config_params == {}


def test_tree_fs_close():
    fs = DummyTreeFileSystem()
    _ = fs.config
    _ = fs.client

    assert not fs.closed

    fs.close()

    assert fs.closed

    with pytest.raises(RuntimeError):
        _ = fs.config

    with pytest.raises(RuntimeError):
        _ = fs.client

    with pytest.raises(RuntimeError):
        with fs:
            pass


def test_tree_fs_serialization():
    fs = DummyTreeFileSystem()
    _ = fs.config
    _ = fs.client

    serialized = pickle.dumps(fs)
    fs2 = pickle.loads(serialized)

    assert isinstance(fs2, DummyTreeFileSystem)
    assert fs is not fs2
    assert fs.config is not fs2.config
    assert fs.client is not fs2.client


def test_tree_fs_threads_are_its_own_and_limited():
    fs = DummyTreeFileSystem(operation_concurrency=2)
    lock = threading.Lock()
    running = 0
    peak = 0

    def call():
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        time.sleep(0.05)
        with lock:
            running -= 1
        return threading.current_thread().name

    async def main():
        return await asyncio.gather(*(fs._run_in_thread(call) for _ in range(6)))

    names = asyncio.run(main())

    assert all(name.startswith("DummyTreeFileSystem") for name in names)
    assert peak == 2
    fs.close()


def test_tree_fs_close_shuts_threads_down():
    fs = DummyTreeFileSystem()
    asyncio.run(fs._run_in_thread(lambda: None))
    executor = fs._executor

    fs.close()

    assert fs._executor is None
    assert executor._shutdown
    # A later call makes new threads.
    assert asyncio.run(fs._run_in_thread(lambda x: x + 1, 1)) == 2
    assert fs._executor is not executor
    fs.close()


def test_tree_fs_serialization_leaves_threads_out():
    fs = DummyTreeFileSystem()
    asyncio.run(fs._run_in_thread(lambda: None))

    fs2 = pickle.loads(pickle.dumps(fs))

    assert fs2._executor is None
    fs.close()

import pickle

import pytest
from databricks.sdk import WorkspaceClient

from fsspec_databricks.base import AbstractDatabricksFileSystem


class DummyDatabricksFileSystem(AbstractDatabricksFileSystem):
    pass


def test_abstract_dbx_fs_init(client: WorkspaceClient):
    fs = DummyDatabricksFileSystem(config_params={"a": "b"})

    assert fs.config is not None
    assert fs.client is not None
    assert fs._config is None
    assert fs._client is None
    assert fs._config_params == {"a": "b"}

    fs = DummyDatabricksFileSystem(config=client.config)
    assert fs.config is client.config
    assert fs.client is not None
    assert fs.client is not client
    assert fs._config is client.config
    assert fs._client is None
    assert fs._config_params == {}

    fs = DummyDatabricksFileSystem(client=client)
    assert fs.config is client.config
    assert fs.client is client
    assert fs._config is None
    assert fs._client is client
    assert fs._config_params == {}

    assert not fs.verbose_debug_log

    fs = DummyDatabricksFileSystem(verbose_debug_log=True)
    assert fs.verbose_debug_log


def test_abstract_dbx_fs_close():
    fs = DummyDatabricksFileSystem(verbose_debug_log=True)
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


def test_abstract_dbx_fs_serialization():
    fs = DummyDatabricksFileSystem(verbose_debug_log=True)
    _ = fs.config
    _ = fs.client

    serialized = pickle.dumps(fs)
    fs2 = pickle.loads(serialized)

    assert isinstance(fs2, DummyDatabricksFileSystem)
    assert fs2.verbose_debug_log
    assert fs is not fs2
    assert fs.config is not fs2.config
    assert fs.client is not fs2.client

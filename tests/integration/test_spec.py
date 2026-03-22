import pickle
from random import randbytes

import fsspec
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from fsspec_databricks import DatabricksFileSystem, use
from tests.integration.test_volume import (
    dbfs_url as volume_dbfs_url,
)
from tests.integration.test_volume import (
    download as volume_download,
)
from tests.integration.test_volume import (
    init_test_dir as init_volume_test_dir,
)
from tests.integration.test_workspace import (
    dbfs_url as workspace_dbfs_url,
)
from tests.integration.test_workspace import (
    download as workspace_download,
)
from tests.integration.test_workspace import (
    init_test_dir as init_workspace_test_dir,
)


def test_use():
    use()

    fs_class = fsspec.get_filesystem_class("dbfs")
    assert fs_class is DatabricksFileSystem

    fs = fsspec.filesystem("dbfs")
    assert isinstance(fs, DatabricksFileSystem)


def test_dbx_fs_init(client: WorkspaceClient):
    with DatabricksFileSystem() as fs:
        assert (
            fs.volume_fs_max_read_concurrency
            == DatabricksFileSystem.volume_fs_max_read_concurrency
        )
        assert (
            fs.volume_fs_min_read_block_size
            == DatabricksFileSystem.volume_fs_min_read_block_size
        )
        assert (
            fs.volume_fs_max_read_block_size
            == DatabricksFileSystem.volume_fs_max_read_block_size
        )

        assert (
            fs.volume_fs_max_write_concurrency
            == DatabricksFileSystem.volume_fs_max_write_concurrency
        )
        assert (
            fs.volume_fs_min_write_block_size
            == DatabricksFileSystem.volume_fs_min_write_block_size
        )
        assert (
            fs.volume_fs_max_write_block_size
            == DatabricksFileSystem.volume_fs_max_write_block_size
        )

        assert (
            fs.volume_min_multipart_upload_size
            == DatabricksFileSystem.volume_min_multipart_upload_size
        )

        assert fs.use_local_fs_in_workspace

    with DatabricksFileSystem(
        volume_fs_max_read_concurrency=1,
        volume_fs_min_read_block_size=2 * 1024 * 1024,
        volume_fs_max_read_block_size=3 * 1024 * 1024,
        volume_fs_max_write_concurrency=4,
        volume_fs_min_write_block_size=5 * 1024 * 1024,
        volume_fs_max_write_block_size=6 * 1024 * 1024,
        volume_min_multipart_upload_size=7 * 1024 * 1024,
        use_local_fs_in_workspace=False,
    ) as fs:
        assert fs.volume_fs_max_read_concurrency == 1
        assert fs.volume_fs_min_read_block_size == 2 * 1024 * 1024
        assert fs.volume_fs_max_read_block_size == 3 * 1024 * 1024

        assert fs.volume_fs_max_write_concurrency == 4
        assert fs.volume_fs_min_write_block_size == 5 * 1024 * 1024
        assert fs.volume_fs_max_write_block_size == 6 * 1024 * 1024

        assert fs.volume_min_multipart_upload_size == 7 * 1024 * 1024

        assert not fs.use_local_fs_in_workspace


def test_dbx_fs_close():
    fs = DatabricksFileSystem(verbose_debug_log=True)
    _ = fs._legacy_fs
    _ = fs._volume_fs
    _ = fs._workspace_fs
    _ = fs._local_fs

    fs.close()

    assert fs.closed

    with pytest.raises(RuntimeError):
        _ = fs._legacy_fs

    with pytest.raises(RuntimeError):
        _ = fs._volume_fs

    with pytest.raises(RuntimeError):
        _ = fs._workspace_fs

    with pytest.raises(RuntimeError):
        _ = fs._local_fs


def test_dbx_fs_serialization():
    fs = DatabricksFileSystem()
    _ = fs._legacy_fs
    _ = fs._volume_fs
    _ = fs._workspace_fs
    _ = fs._local_fs

    serialized = pickle.dumps(fs)
    fs2 = pickle.loads(serialized)

    assert isinstance(fs2, DatabricksFileSystem)
    assert fs is not fs2
    assert fs._legacy_fs is not fs2._legacy_fs
    assert fs._volume_fs is not fs2._volume_fs
    assert fs._workspace_fs is not fs2._workspace_fs
    assert fs._local_fs is fs2._local_fs  # Local FS is cacheable


def test_dbx_fs_copy_file(
    client: WorkspaceClient, volume_test_root, workspace_test_root
):
    data_vol = randbytes(65536)
    data_ws = randbytes(65536)

    volume_test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        items={"file1.bin": data_vol},
    )

    workspace_test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={"file2.bin": (data_ws, ImportFormat.RAW, None)},
    )

    fs = DatabricksFileSystem(client=client)

    src = volume_test_dir + "/file1.bin"
    dest = workspace_test_dir + "/file1.bin"
    fs.cp_file(volume_dbfs_url(src), workspace_dbfs_url(dest))
    assert workspace_download(client, dest) == data_vol

    src = workspace_test_dir + "/file2.bin"
    dest = volume_test_dir + "/file2.bin"

    fs.cp_file(workspace_dbfs_url(src), volume_dbfs_url(dest))
    assert volume_download(client, dest) == data_ws


def test_dbx_fs_copy(client: WorkspaceClient, volume_test_root, workspace_test_root):
    data_vol1 = randbytes(65536)
    data_vol2 = randbytes(65536)
    data_ws1 = randbytes(65536)
    data_ws2 = randbytes(65536)

    volume_test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        items={
            "dir_1": {
                "file1.bin": data_vol1,
                "file2.bin": data_vol2,
            }
        },
    )

    workspace_test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={
            "dir_2": {
                "file1.bin": (data_ws1, ImportFormat.RAW, None),
                "file2.bin": (data_ws2, ImportFormat.RAW, None),
            }
        },
    )

    fs = DatabricksFileSystem(client=client)

    src = volume_test_dir + "/dir_1"
    dest = workspace_test_dir + "/dir_1"
    fs.copy(volume_dbfs_url(src), workspace_dbfs_url(dest), recursive=True)
    assert workspace_download(client, dest + "/file1.bin") == data_vol1
    assert workspace_download(client, dest + "/file2.bin") == data_vol2

    src = workspace_test_dir + "/dir_2"
    dest = volume_test_dir + "/dir_2"

    fs.copy(workspace_dbfs_url(src), volume_dbfs_url(dest), recursive=True)
    assert volume_download(client, dest + "/file1.bin") == data_ws1
    assert volume_download(client, dest + "/file2.bin") == data_ws2


def test_dbx_fs_move(client: WorkspaceClient, volume_test_root, workspace_test_root):
    data_vol_1 = randbytes(65536)
    data_vol_3 = randbytes(65536)
    data_fs_4 = randbytes(65536)
    data_fs_6 = randbytes(65536)

    volume_test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        items={
            "file1.bin": data_vol_1,
            "dir_2": {
                "file3.bin": data_vol_3,
            },
        },
    )

    workspace_test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={
            "file4.bin": (data_fs_4, ImportFormat.RAW, None),
            "dir_5": {
                "file6.bin": (data_fs_6, ImportFormat.RAW, None),
            },
        },
    )

    fs = DatabricksFileSystem(client=client)

    src = volume_test_dir + "/file1.bin"
    dest = workspace_test_dir + "/file1.bin"

    fs.move(volume_dbfs_url(src), workspace_dbfs_url(dest))
    assert not fs.exists(volume_dbfs_url(src))
    assert workspace_download(client, dest) == data_vol_1

    src = volume_test_dir + "/dir_2"
    dest = workspace_test_dir + "/dir_2"

    fs.move(volume_dbfs_url(src), workspace_dbfs_url(dest), recursive=True)
    assert not fs.exists(volume_dbfs_url(src))
    assert fs.exists(workspace_dbfs_url(dest))
    assert workspace_download(client, dest + "/file3.bin") == data_vol_3

    src = workspace_test_dir + "/file4.bin"
    dest = volume_test_dir + "/file4.bin"

    fs.move(workspace_dbfs_url(src), volume_dbfs_url(dest))
    assert not fs.exists(workspace_dbfs_url(src))
    assert volume_download(client, dest) == data_fs_4

    src = workspace_test_dir + "/dir_5"
    dest = volume_test_dir + "/dir_5"

    fs.move(workspace_dbfs_url(src), volume_dbfs_url(dest), recursive=True)
    assert not fs.exists(workspace_dbfs_url(src))
    assert fs.exists(volume_dbfs_url(dest))
    assert volume_download(client, dest + "/file6.bin") == data_fs_6

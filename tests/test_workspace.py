"""Tests of `WorkspaceFileSystem`, through the SDK over HTTP: on the dummy server, and on the live workspace."""

import errno
import pickle
from datetime import UTC, datetime, timedelta
from random import randbytes
from time import sleep

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectType,
)
from fsspec.asyn import sync
from fsspec_alt_strategies.monitor import Monitor, monitoring

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks.workspace import WorkspaceFileSystem
from tests.utils import bytes_sig, init_workspace_test_dir, workspace_download

pytestmark = pytest.mark.backend("workspace")


def dbfs_url(path: str) -> str:
    return f"dbfs:/Workspace/{path.lstrip('/')}"


def stripped_dbfs_url(path: str) -> str:
    return f"/Workspace/{path.lstrip('/')}"


def status(client: WorkspaceClient, path: str):
    return client.workspace.get_status(path)


def test_workspace_init():
    with WorkspaceFileSystem() as _:
        # No additional params. Just test creation of new instance
        pass


def test_workspace_close():
    fs = WorkspaceFileSystem()

    fs.close()
    assert fs.closed


def test_workspace_serialization():
    fs = WorkspaceFileSystem()

    serialized = pickle.dumps(fs)
    fs2 = pickle.loads(serialized)

    assert isinstance(fs2, WorkspaceFileSystem)
    assert fs is not fs2


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_info(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nSELECT * FROM TEST"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (data, ImportFormat.RAW, None),
            "2_notebook": (notebook, ImportFormat.SOURCE, Language.SQL),
            "3_directory": {},
        },
    )

    data_path = f"{test_dir}/1_data.bin"
    info = fs.info(dbfs_url(data_path))

    assert info["name"] == stripped_dbfs_url(data_path)
    assert info["size"] == len(data)
    assert info["type"] == "file"
    assert datetime.now(tz=UTC) - info["created"] < timedelta(minutes=1)
    assert datetime.now(tz=UTC) - info["modified"] < timedelta(minutes=1)
    assert info["islink"] is False
    assert info["object_type"] == "FILE"
    assert info["language"] is None
    assert info["object_id"] is not None
    assert info["resource_id"] is not None
    assert fs.created(dbfs_url(data_path)) == info["created"]
    assert fs.modified(dbfs_url(data_path)) == info["modified"]

    notebook_path = f"{test_dir}/2_notebook"
    info = fs.info(dbfs_url(notebook_path))

    assert info["name"] == stripped_dbfs_url(notebook_path)
    assert info["size"] is None
    assert info["type"] == "file"
    assert info["object_type"] == "NOTEBOOK"
    assert info["language"] == "SQL"

    directory_path = f"{test_dir}/3_directory"
    info = fs.info(dbfs_url(directory_path))

    assert info["name"] == stripped_dbfs_url(directory_path)
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] is None
    assert info["modified"] is None
    assert info["islink"] is False
    assert info["object_type"] == "DIRECTORY"
    assert info["language"] is None
    assert info["object_id"] is not None
    assert info["resource_id"] is not None

    with pytest.raises(FileNotFoundError):
        fs.info(dbfs_url(f"{test_dir}/nonexistent"))


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_ls(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\npython"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (data, ImportFormat.RAW, None),
            "2_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "3_directory": {},
        },
    )

    items: list[dict] = fs.ls(stripped_dbfs_url(test_dir), detail=True)
    assert len(items) == 3

    items.sort(key=lambda i: i["name"])
    assert items[0]["name"] == stripped_dbfs_url(f"{test_dir}/1_data.bin")
    assert items[0]["size"] == len(data)
    assert items[0]["type"] == "file"
    assert datetime.now(tz=UTC) - items[0]["created"] < timedelta(minutes=1)
    assert datetime.now(tz=UTC) - items[0]["modified"] < timedelta(minutes=1)
    assert items[0]["islink"] is False
    assert items[0]["object_type"] == "FILE"
    assert items[0]["language"] is None
    assert items[0]["object_id"] is not None
    assert items[0]["resource_id"] is not None

    assert items[1]["name"] == stripped_dbfs_url(f"{test_dir}/2_notebook")
    assert items[1]["size"] is None
    assert items[1]["type"] == "file"
    assert datetime.now(tz=UTC) - items[1]["created"] < timedelta(minutes=1)
    assert datetime.now(tz=UTC) - items[1]["modified"] < timedelta(minutes=1)
    assert items[1]["islink"] is False
    assert items[1]["object_type"] == "NOTEBOOK"
    assert items[1]["language"] == "PYTHON"
    assert items[1]["object_id"] is not None
    assert items[1]["resource_id"] is not None

    assert items[2]["name"] == stripped_dbfs_url(f"{test_dir}/3_directory")
    assert items[2]["size"] is None
    assert items[2]["type"] == "directory"
    assert items[2]["created"] is None
    assert items[2]["modified"] is None
    assert items[2]["islink"] is False
    assert items[2]["object_type"] == "DIRECTORY"
    assert items[2]["language"] is None
    assert items[2]["object_id"] is not None
    assert items[2]["resource_id"] is not None

    assert fs.ls(stripped_dbfs_url(test_dir), detail=False) == [
        stripped_dbfs_url(f"{test_dir}/1_data.bin"),
        stripped_dbfs_url(f"{test_dir}/2_notebook"),
        stripped_dbfs_url(f"{test_dir}/3_directory"),
    ]

    with pytest.raises(FileNotFoundError):
        fs.ls(dbfs_url(f"{test_dir}/nonexistent"))


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_exists(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (randbytes(12345), ImportFormat.RAW, None),
            "2_notebook": (
                b"# Databricks notebook source\npython",
                ImportFormat.SOURCE,
                Language.PYTHON,
            ),
            "3_directory": {},
        },
    )

    assert fs.exists(dbfs_url(f"{test_dir}/1_data.bin"))
    assert fs.exists(dbfs_url(f"{test_dir}/2_notebook"))
    assert fs.exists(dbfs_url(f"{test_dir}/3_directory"))
    assert not fs.exists(dbfs_url(f"{test_dir}/nonexistent"))


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_cp_file(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (data, ImportFormat.RAW, None),
            "2_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "3_directory": {},
        },
    )

    # Test copying a file
    fs.cp_file(
        dbfs_url(f"{test_dir}/1_data.bin"),
        dbfs_url(f"{test_dir}/1_data_copied.bin"),
    )
    copied_data = workspace_download(client, f"{test_dir}/1_data_copied.bin")

    assert bytes_sig(data) == bytes_sig(copied_data)

    # Test copying a notebook
    fs.cp_file(
        dbfs_url(f"{test_dir}/2_notebook"),
        dbfs_url(f"{test_dir}/2_notebook_copied"),
    )
    copied_notebook = workspace_download(
        client, f"{test_dir}/2_notebook_copied", ExportFormat.SOURCE
    )

    assert notebook == copied_notebook

    # Test copying a directory
    fs.cp_file(
        dbfs_url(f"{test_dir}/3_directory"),
        dbfs_url(f"{test_dir}/3_directory_copied"),
    )

    copied_dir = status(client, f"{test_dir}/3_directory_copied")
    assert copied_dir.object_type == ObjectType.DIRECTORY

    # Test copying not existent path
    with pytest.raises(FileNotFoundError):
        fs.cp_file(
            dbfs_url(f"{test_dir}/4_nonexistent"),
            dbfs_url(f"{test_dir}/4_nonexistent_copy"),
        )


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_copy(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data1 = randbytes(12345)
    data2 = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "directory": {
                "1_data.bin": (data1, ImportFormat.RAW, None),
                "2_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
                "3_directory": {
                    "5_data.bin": (data2, ImportFormat.RAW, None),
                },
                "4_empty_directory": {},
            }
        },
    )

    fs.copy(
        dbfs_url(f"{test_dir}/directory"),
        dbfs_url(f"{test_dir}/directory_copied"),
        recursive=True,
    )

    items = [(i.path, i.object_type) for i in client.workspace.list(test_dir)]
    assert items == [
        (f"{test_dir}/directory", ObjectType.DIRECTORY),
        (f"{test_dir}/directory_copied", ObjectType.DIRECTORY),
    ]

    # Sorted: the API lists in creation order, and the copy creates concurrently.
    items = sorted(
        (i.path, i.object_type, i.language)
        for i in client.workspace.list(f"{test_dir}/directory_copied")
    )
    assert items == [
        (f"{test_dir}/directory_copied/1_data.bin", ObjectType.FILE, None),
        (
            f"{test_dir}/directory_copied/2_notebook",
            ObjectType.NOTEBOOK,
            Language.PYTHON,
        ),
        (f"{test_dir}/directory_copied/3_directory", ObjectType.DIRECTORY, None),
        (f"{test_dir}/directory_copied/4_empty_directory", ObjectType.DIRECTORY, None),
    ]

    items = [
        (i.path, i.object_type, i.language)
        for i in client.workspace.list(f"{test_dir}/directory_copied/3_directory")
    ]
    assert items == [
        (f"{test_dir}/directory_copied/3_directory/5_data.bin", ObjectType.FILE, None),
    ]

    items = [
        (i.path, i.object_type, i.language)
        for i in client.workspace.list(f"{test_dir}/directory_copied/4_empty_directory")
    ]
    assert len(items) == 0


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_rm(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (data, ImportFormat.RAW, None),
            "2_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "3_directory": {
                "4_data.bin": (data, ImportFormat.RAW, None),
            },
        },
    )

    fs.rm(dbfs_url(f"{test_dir}/1_data.bin"))
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/1_data.bin")

    fs.rm(dbfs_url(f"{test_dir}/2_notebook"))
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/2_notebook")

    with pytest.raises(IsADirectoryError):
        fs.rm(dbfs_url(f"{test_dir}/3_directory"), recursive=False)

    fs.rm(dbfs_url(f"{test_dir}/3_directory"), recursive=True)
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/3_directory")

    with pytest.raises(FileNotFoundError):
        fs.rm(dbfs_url(f"{test_dir}/nonexistent"))


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_mkdir(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_workspace_test_dir(client, workspace_test_root, fs_class)

    fs.mkdir(dbfs_url(f"{test_dir}/test_dir"))

    status1 = status(client, f"{test_dir}/test_dir")
    assert status1.object_type == ObjectType.DIRECTORY

    with pytest.raises(FileExistsError):
        fs.mkdir(dbfs_url(f"{test_dir}/test_dir"))

    with pytest.raises(FileNotFoundError):
        fs.mkdir(dbfs_url(f"{test_dir}/parent_dir/dir"), create_parents=False)

    fs.mkdir(dbfs_url(f"{test_dir}/parent_dir/dir"), create_parents=True)
    status2 = status(client, f"{test_dir}/parent_dir/dir")

    assert status2.object_type == ObjectType.DIRECTORY


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_makedirs(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_workspace_test_dir(client, workspace_test_root, fs_class)

    fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"))
    status1 = status(client, f"{test_dir}/parent_dir/dir")

    assert status1.object_type == ObjectType.DIRECTORY

    with pytest.raises(FileExistsError):
        fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"), exist_ok=False)

    sleep(0.1)
    fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"), exist_ok=True)
    status2 = status(client, f"{test_dir}/parent_dir/dir")

    assert (
        status1.created_at
        == status1.modified_at
        == status2.created_at
        == status2.modified_at
    )


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_rmdir(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_directory": {},
            "2_data.bin": (randbytes(12345), ImportFormat.RAW, None),
            "3_nonempty_directory": {
                "4_data.bin": (randbytes(12345), ImportFormat.RAW, None),
            },
        },
    )

    fs.rmdir(dbfs_url(f"{test_dir}/1_directory"))
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/1_directory")

    with pytest.raises(FileNotFoundError):
        fs.rmdir(dbfs_url(f"{test_dir}/nonexistent"))

    with pytest.raises(NotADirectoryError):
        fs.rmdir(dbfs_url(f"{test_dir}/2_data.bin"))

    with pytest.raises(OSError) as exc_info:
        fs.rmdir(dbfs_url(f"{test_dir}/3_nonempty_directory"))
    assert exc_info.value.errno == errno.ENOTEMPTY


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_open_r(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (data, ImportFormat.RAW, None),
            "2_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "3_directory": {},
        },
    )

    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="rb") as f:
        read_data = f.read()

    assert bytes_sig(data) == bytes_sig(read_data)

    with fs.open(dbfs_url(f"{test_dir}/2_notebook"), mode="rb") as f:
        read_notebook = f.read()

    assert notebook == read_notebook

    with pytest.raises(IsADirectoryError):
        fs.open(dbfs_url(f"{test_dir}/3_directory"), mode="rb")

    with pytest.raises(FileNotFoundError):
        fs.open(dbfs_url(f"{test_dir}/nonexistent"), mode="rb")


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_open_w(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "3_data.bin": (data, ImportFormat.RAW, None),
            "4_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "5_directory": {},
        },
    )

    written_data = randbytes(54321)
    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
        f.write(written_data)

    read_data = workspace_download(client, f"{test_dir}/1_data.bin")
    assert bytes_sig(written_data) == bytes_sig(read_data)

    written_notebook = b"# Databricks notebook source\nval a"

    with (
        pytest.raises(
            ValueError, match="language must be specified when writing a notebook"
        ),
        fs.open(
            dbfs_url(f"{test_dir}/2_notebook"),
            mode="wb",
            notebook_format="SOURCE",
        ) as f,
    ):
        f.write(written_notebook)

    with fs.open(
        dbfs_url(f"{test_dir}/2_notebook"),
        mode="wb",
        notebook_format="SOURCE",
        language="scala",
    ) as f:
        f.write(written_notebook)

    read_status = status(client, f"{test_dir}/2_notebook")
    read_notebook = workspace_download(
        client, f"{test_dir}/2_notebook", ExportFormat.SOURCE
    )
    assert read_status.object_type == ObjectType.NOTEBOOK
    assert read_status.language == Language.SCALA
    assert written_notebook == read_notebook

    # Overwrite existing file
    with fs.open(dbfs_url(f"{test_dir}/3_data.bin"), mode="wb") as f:
        f.write(written_data)

    read_data = workspace_download(client, f"{test_dir}/3_data.bin")
    assert bytes_sig(written_data) == bytes_sig(read_data)

    # Overwrite existing notebook
    with fs.open(dbfs_url(f"{test_dir}/4_notebook"), mode="wb") as f:
        f.write(written_notebook)

    read_status = status(client, f"{test_dir}/4_notebook")
    read_notebook = workspace_download(
        client, f"{test_dir}/4_notebook", ExportFormat.SOURCE
    )
    assert read_status.object_type == ObjectType.NOTEBOOK
    # Keep the language of the existing notebook
    assert read_status.language == Language.PYTHON
    assert written_notebook == read_notebook

    # Overwrite existing notebook, with explicitly specified language
    with fs.open(
        dbfs_url(f"{test_dir}/4_notebook"),
        mode="wb",
        language="SCALA",
    ) as f:
        f.write(written_notebook)

    read_status = status(client, f"{test_dir}/4_notebook")
    read_notebook = workspace_download(
        client, f"{test_dir}/4_notebook", ExportFormat.SOURCE
    )
    assert read_status.object_type == ObjectType.NOTEBOOK
    # Keep the language of the existing notebook
    assert read_status.language == Language.SCALA
    assert written_notebook == read_notebook

    with pytest.raises(IsADirectoryError):
        with fs.open(dbfs_url(f"{test_dir}/5_directory"), mode="wb") as f:
            f.write(written_data)


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_open_a(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "3_data.bin": (data, ImportFormat.RAW, None),
            "4_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "5_directory": {},
        },
    )

    written_data = randbytes(54321)
    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="ab") as f:
        f.write(written_data)

    read_data = workspace_download(client, f"{test_dir}/1_data.bin")
    assert bytes_sig(written_data) == bytes_sig(read_data)

    written_notebook = b"# Databricks notebook source\nval a"

    with (
        pytest.raises(
            ValueError, match="language must be specified when writing a notebook"
        ),
        fs.open(
            dbfs_url(f"{test_dir}/2_notebook"),
            mode="ab",
            notebook_format="SOURCE",
        ) as f,
    ):
        f.write(written_notebook)

    with fs.open(
        dbfs_url(f"{test_dir}/2_notebook"),
        mode="ab",
        notebook_format="SOURCE",
        language="scala",
    ) as f:
        f.write(written_notebook)

    read_status = status(client, f"{test_dir}/2_notebook")
    read_notebook = workspace_download(
        client, f"{test_dir}/2_notebook", ExportFormat.SOURCE
    )
    assert read_status.object_type == ObjectType.NOTEBOOK
    assert read_status.language == Language.SCALA
    assert written_notebook == read_notebook

    # Append to existing file
    with fs.open(dbfs_url(f"{test_dir}/3_data.bin"), mode="ab") as f:
        f.write(written_data)

    read_data = workspace_download(client, f"{test_dir}/3_data.bin")
    assert bytes_sig(data + written_data) == bytes_sig(read_data)

    # Append to existing notebook
    append_lines = b"\n# COMMAND ----------\nprint()"
    with fs.open(dbfs_url(f"{test_dir}/4_notebook"), mode="ab") as f:
        f.write(append_lines)

    read_status = status(client, f"{test_dir}/4_notebook")
    read_notebook = workspace_download(
        client, f"{test_dir}/4_notebook", ExportFormat.SOURCE
    )
    assert read_status.object_type == ObjectType.NOTEBOOK
    assert read_status.language == Language.PYTHON
    assert notebook + append_lines == read_notebook

    with pytest.raises(IsADirectoryError):
        with fs.open(dbfs_url(f"{test_dir}/5_directory"), mode="ab") as f:
            f.write(written_data)


@pytest.mark.parametrize(
    "fs_class",
    [
        WorkspaceFileSystem,
        DatabricksFileSystem,
    ],
)
def test_workspace_open_x(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        fs_class,
        items={
            "1_data.bin": (data, ImportFormat.RAW, None),
            "2_notebook": (notebook, ImportFormat.SOURCE, Language.PYTHON),
            "3_directory": {},
        },
    )

    with pytest.raises(FileExistsError):
        with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="xb") as _:
            pass

    with (
        pytest.raises(FileExistsError),
        fs.open(
            dbfs_url(f"{test_dir}/2_notebook"),
            mode="xb",
            notebook_format="SOURCE",
            language="scala",
        ) as _,
    ):
        pass

    with pytest.raises(IsADirectoryError):
        with fs.open(dbfs_url(f"{test_dir}/3_directory"), mode="xb") as _:
            pass

    written_data = randbytes(54321)
    with fs.open(dbfs_url(f"{test_dir}/4_data.bin"), mode="xb") as f:
        f.write(written_data)
    assert bytes_sig(workspace_download(client, f"{test_dir}/4_data.bin")) == bytes_sig(
        written_data
    )

    written_notebook = b"# Databricks notebook source\nval a"
    with fs.open(
        dbfs_url(f"{test_dir}/5_notebook"),
        mode="xb",
        notebook_format="SOURCE",
        language="scala",
    ) as f:
        f.write(written_notebook)
    assert (
        workspace_download(client, f"{test_dir}/5_notebook", ExportFormat.SOURCE)
        == written_notebook
    )


@pytest.mark.parametrize("fs_class", [WorkspaceFileSystem, DatabricksFileSystem])
def test_workspace_transaction(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    test_dir = init_workspace_test_dir(client, workspace_test_root, fs_class)
    fs = fs_class(client=client)
    data = randbytes(54321)

    # Commit uploads the files, none before.
    with fs.transaction:
        with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
            f.write(data)
        with pytest.raises(ResourceDoesNotExist):
            client.workspace.get_status(f"{test_dir}/1_data.bin")
    assert bytes_sig(workspace_download(client, f"{test_dir}/1_data.bin")) == bytes_sig(
        data
    )

    # An exception leaves nothing.
    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        with fs.open(dbfs_url(f"{test_dir}/2_data.bin"), mode="wb") as f:
            f.write(data)
        raise ValueError("dummy")
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/2_data.bin")


### Requests, on the dummy server


class Calls(Monitor):
    """Records the calls whose operations it is told of."""

    def __init__(self):
        self.calls = []

    async def started(self, call, state, op):
        self.calls.append(call)


NOTEBOOK = b"# Databricks notebook source\n# nb"


@pytest.fixture
def tree(client, workspace_test_root, dummy, request):
    """A test directory of files, a notebook and folders, and a `WorkspaceFileSystem`, with no request recorded."""
    test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={
            "data.bin": (b"data", ImportFormat.RAW, None),
            "empty": {},
            "dir": {
                "a.bin": (b"a", ImportFormat.RAW, None),
                "nb": (NOTEBOOK, ImportFormat.SOURCE, Language.PYTHON),
                "sub": {"b.bin": (b"b", ImportFormat.RAW, None)},
            },
        },
    )
    with WorkspaceFileSystem(client=client) as fs:
        dummy.requests.clear()
        yield fs, test_dir


def _requests(dummy, endpoint):
    return [r.params for r in dummy.requests_to(f"/api/2.0/workspace/{endpoint}")]


def test_workspace_root(client):
    fs = WorkspaceFileSystem(client=client)
    assert fs.info("/")["type"] == "directory"
    assert fs.ls("/", detail=False) == ["/Workspace"]
    with pytest.raises(FileNotFoundError):
        fs.info("/Volumes/c/s/v")


def test_workspace_find_runs_tree_strategy(tree, dummy):
    """`find` is the tree strategy: one listing per folder, seen by monitors."""
    fs, test_dir = tree
    monitor = Calls()
    with monitoring(monitor):
        found = fs.find(f"/Workspace{test_dir}/dir")

    assert found == [
        f"/Workspace{test_dir}/dir/a.bin",
        f"/Workspace{test_dir}/dir/nb",
        f"/Workspace{test_dir}/dir/sub/b.bin",
    ]
    assert [(c.fs, c.method) for c in monitor.calls] == [(fs, "find")]
    assert sorted(p["path"] for p in _requests(dummy, "list")) == [
        f"{test_dir}/dir",
        f"{test_dir}/dir/sub",
    ]


def test_workspace_rm_recursive_is_native(tree, dummy):
    """A recursive `rm` without patterns is the API's own recursive delete: one request."""
    fs, test_dir = tree
    fs.rm(f"/Workspace{test_dir}/dir", recursive=True)

    assert _requests(dummy, "delete") == [
        {"path": f"{test_dir}/dir", "recursive": True}
    ]
    assert not fs.exists(f"/Workspace{test_dir}/dir")


def test_workspace_rm_glob_runs_tree_strategy(tree, dummy):
    fs, test_dir = tree
    fs.rm(f"/Workspace{test_dir}/dir/*.bin", recursive=True)

    assert not fs.exists(f"/Workspace{test_dir}/dir/a.bin")
    assert fs.exists(f"/Workspace{test_dir}/dir/nb")
    assert {"path": f"{test_dir}/dir/a.bin", "recursive": False} in _requests(
        dummy, "delete"
    )


def test_workspace_rm_refuses_directories(tree, dummy):
    fs, test_dir = tree
    with pytest.raises(IsADirectoryError):
        fs.rm(f"/Workspace{test_dir}/empty")
    with pytest.raises(IsADirectoryError):
        fs.rm_file(f"/Workspace{test_dir}/empty")
    with pytest.raises(NotADirectoryError):
        fs.makedirs(f"/Workspace{test_dir}/data.bin", exist_ok=True)

    assert _requests(dummy, "delete") == []


def test_workspace_copy_exports_notebooks_as_source(tree, dummy):
    fs, test_dir = tree
    fs.copy(f"/Workspace{test_dir}/dir", f"/Workspace{test_dir}/copy", recursive=True)

    nb = fs.info(f"/Workspace{test_dir}/copy/nb")
    assert (nb["object_type"], nb["language"]) == ("NOTEBOOK", "PYTHON")
    exports = [p for p in _requests(dummy, "export") if p["path"].endswith("/nb")]
    assert [p["format"] for p in exports] == ["SOURCE"]


def test_workspace_open_import_params(tree, dummy):
    """New files are imported RAW without overwrite, existing notebooks as SOURCE in their language."""
    fs, test_dir = tree
    with fs.open(f"/Workspace{test_dir}/new.bin", "xb") as f:
        f.write(b"new")
    with fs.open(f"/Workspace{test_dir}/dir/nb", "wb") as f:
        f.write(b"# Databricks notebook source\n# changed")

    imports = [
        {k: p.get(k) for k in ("path", "format", "language", "overwrite")}
        for p in _requests(dummy, "import")
    ]
    assert imports == [
        {
            "path": f"{test_dir}/new.bin",
            "format": "RAW",
            "language": None,
            "overwrite": None,
        },
        {
            "path": f"{test_dir}/dir/nb",
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": "true",
        },
    ]


def test_workspace_file_size_limit(tree, dummy):
    fs, test_dir = tree
    with (
        pytest.raises(OSError) as e,
        fs.open(f"/Workspace{test_dir}/big.bin", "wb") as f,
    ):
        f.write(b"x" * (10 * 1024 * 1024 + 1))
    assert e.value.errno == errno.EFBIG
    assert not fs.exists(f"/Workspace{test_dir}/big.bin")


def test_workspace_open_async(tree):
    fs, test_dir = tree

    async def read():
        f = await fs.open_async(f"/Workspace{test_dir}/data.bin")
        async with f:
            assert f.size == 4
            f.seek(2)
            return await f.read()

    assert sync(fs.loop, read) == b"ta"


def test_workspace_transaction_uploads_on_commit(tree, dummy):
    """A file written in a transaction is uploaded on commit only, and never on discard."""
    fs, test_dir = tree
    with fs.transaction:
        with fs.open(f"/Workspace{test_dir}/data.bin", "ab") as f:
            f.write(b"+more")
        assert _requests(dummy, "import") == []
    assert fs.cat_file(f"/Workspace{test_dir}/data.bin") == b"data+more"

    dummy.requests.clear()
    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        with fs.open(f"/Workspace{test_dir}/lost.bin", "wb") as f:
            f.write(b"lost")
        raise ValueError("dummy")
    assert _requests(dummy, "import") == []

import errno
import inspect
import logging
import pickle
from datetime import datetime, timedelta, timezone
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

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks.workspace import WorkspaceFileSystem
from tests.utils import bytes_sig

log = logging.getLogger(__name__)


def init_test_dir(
    client: WorkspaceClient,
    test_root: str,
    fs_class: type | None = None,
    name: str | None = None,
    items: dict[str, dict | tuple] | None = None,
) -> str:
    """Initialize a test directory in the Databricks workspace with the given items."""
    name = name or inspect.stack()[1].function

    if fs_class:
        test_dir = f"{test_root}/{name}_{fs_class.__name__}"
    else:
        test_dir = f"{test_root}/{name}"

    try:
        log.debug("Checking test dir: path=%s", test_dir)
        client.workspace.get_status(test_dir)
        log.debug("Deleting test dir: path=%s", test_dir)
        client.workspace.delete(test_dir, recursive=True)
        log.debug("Deleted test dir: path=%s", test_dir)
    except ResourceDoesNotExist:
        pass

    log.debug("Creating test dir: path=%s", test_dir)
    client.workspace.mkdirs(test_dir)
    log.debug("Created test dir: path=%s", test_dir)

    def init(path: str, _items: dict[str, dict | tuple]):
        for n, i in _items.items():
            item_path = f"{path}/{n}"
            if isinstance(i, dict):
                client.workspace.mkdirs(item_path)
                init(item_path, i)
            else:
                content, fmt, language = i
                client.workspace.upload(
                    item_path, content, format=fmt, language=language
                )

    if items:
        log.debug("Creating files/directories in the test dir: path=%s", test_dir)
        init(test_dir, items)
        log.debug("Created files/directories in the test dir: path=%s", test_dir)

    return test_dir


def dbfs_url(path: str) -> str:
    return f"dbfs:/Workspace/{path.lstrip('/')}"


def stripped_dbfs_url(path: str) -> str:
    return f"/Workspace/{path.lstrip('/')}"


def status(client: WorkspaceClient, path: str):
    """Get the status of the file or directory at the given path in the Databricks workspace."""
    return client.workspace.get_status(path)


def download(client: WorkspaceClient, path: str) -> bytes:
    """Download the file at the given path from the Databricks workspace."""
    with client.workspace.download(path, format=ExportFormat.AUTO) as f:
        return f.read()


def nb_download(client: WorkspaceClient, path: str) -> bytes:
    """Download the notebook at the given path from the Databricks workspace."""
    with client.workspace.download(path, format=ExportFormat.SOURCE) as f:
        return f.read()


def test_workspace_fs_init(client: WorkspaceClient):
    with WorkspaceFileSystem() as _:
        # No additional params. Just test creation of new instance
        pass


def test_workspace_fs_close():
    fs = WorkspaceFileSystem(verbose_debug_log=True)

    fs.close()
    assert fs.closed


def test_workspace_fs_serialization():
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
def test_workspace_fs_info(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nSELECT * FROM TEST"

    test_dir = init_test_dir(
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
    assert datetime.now(tz=timezone.utc) - info["created"] < timedelta(minutes=1)
    assert datetime.now(tz=timezone.utc) - info["modified"] < timedelta(minutes=1)
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
def test_workspace_fs_ls(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\npython"

    test_dir = init_test_dir(
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
    assert datetime.now(tz=timezone.utc) - items[0]["created"] < timedelta(minutes=1)
    assert datetime.now(tz=timezone.utc) - items[0]["modified"] < timedelta(minutes=1)
    assert items[0]["islink"] is False
    assert items[0]["object_type"] == "FILE"
    assert items[0]["language"] is None
    assert items[0]["object_id"] is not None
    assert items[0]["resource_id"] is not None

    assert items[1]["name"] == stripped_dbfs_url(f"{test_dir}/2_notebook")
    assert items[1]["size"] is None
    assert items[1]["type"] == "file"
    assert datetime.now(tz=timezone.utc) - items[1]["created"] < timedelta(minutes=1)
    assert datetime.now(tz=timezone.utc) - items[1]["modified"] < timedelta(minutes=1)
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
def test_workspace_fs_exists(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(
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
def test_workspace_fs_cp_file(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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
    copied_data = download(client, f"{test_dir}/1_data_copied.bin")

    assert bytes_sig(data) == bytes_sig(copied_data)

    # Test copying a notebook
    fs.cp_file(
        dbfs_url(f"{test_dir}/2_notebook"),
        dbfs_url(f"{test_dir}/2_notebook_copied"),
    )
    copied_notebook = nb_download(client, f"{test_dir}/2_notebook_copied")

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
def test_workspace_fs_copy(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data1 = randbytes(12345)
    data2 = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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

    items = [
        (i.path, i.object_type, i.language)
        for i in client.workspace.list(f"{test_dir}/directory_copied")
    ]
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
def test_workspace_fs_rm(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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
def test_workspace_fs_mkdir(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(client, workspace_test_root, fs_class)

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
def test_workspace_fs_makedirs(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(client, workspace_test_root, fs_class)

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
def test_workspace_fs_rmdir(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(
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
def test_workspace_fs_open_r(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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
def test_workspace_fs_open_w(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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

    read_data = download(client, f"{test_dir}/1_data.bin")
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
    read_notebook = nb_download(client, f"{test_dir}/2_notebook")
    assert read_status.object_type == ObjectType.NOTEBOOK
    assert read_status.language == Language.SCALA
    assert written_notebook == read_notebook

    # Overwrite existing file
    with fs.open(dbfs_url(f"{test_dir}/3_data.bin"), mode="wb") as f:
        f.write(written_data)

    read_data = download(client, f"{test_dir}/3_data.bin")
    assert bytes_sig(written_data) == bytes_sig(read_data)

    # Overwrite existing notebook
    with fs.open(dbfs_url(f"{test_dir}/4_notebook"), mode="wb") as f:
        f.write(written_notebook)

    read_status = status(client, f"{test_dir}/4_notebook")
    read_notebook = nb_download(client, f"{test_dir}/4_notebook")
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
    read_notebook = nb_download(client, f"{test_dir}/4_notebook")
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
def test_workspace_fs_open_a(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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

    read_data = download(client, f"{test_dir}/1_data.bin")
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
    read_notebook = nb_download(client, f"{test_dir}/2_notebook")
    assert read_status.object_type == ObjectType.NOTEBOOK
    assert read_status.language == Language.SCALA
    assert written_notebook == read_notebook

    # Append to existing file
    with fs.open(dbfs_url(f"{test_dir}/3_data.bin"), mode="ab") as f:
        f.write(written_data)

    read_data = download(client, f"{test_dir}/3_data.bin")
    assert bytes_sig(data + written_data) == bytes_sig(read_data)

    # Append to existing notebook
    append_lines = b"\n# COMMAND ----------\nprint()"
    with fs.open(dbfs_url(f"{test_dir}/4_notebook"), mode="ab") as f:
        f.write(append_lines)

    read_status = status(client, f"{test_dir}/4_notebook")
    read_notebook = nb_download(client, f"{test_dir}/4_notebook")
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
def test_workspace_fs_open_x(
    client: WorkspaceClient,
    fs_class: type[WorkspaceFileSystem | DatabricksFileSystem],
    workspace_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)
    notebook = b"# Databricks notebook source\nspark"

    test_dir = init_test_dir(
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
    assert bytes_sig(download(client, f"{test_dir}/4_data.bin")) == bytes_sig(
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
    assert nb_download(client, f"{test_dir}/5_notebook") == written_notebook

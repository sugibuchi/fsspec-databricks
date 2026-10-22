import pytest

from fsspec_databricks.path import (
    FileSystemType,
    from_local_path,
    fs_type,
    parse_dbfs_path,
    parse_volume_path,
    parse_workspace_path,
    strip_scheme,
    to_local_path,
    unstrip_scheme,
)


def test_strip_and_unstrip_scheme():
    assert strip_scheme("dbfs:/a/b") == "/a/b"
    assert strip_scheme("dbfs://a/b") == "/a/b"
    assert strip_scheme("/a/b") == "/a/b"

    assert strip_scheme("dbfs:/") == "/"
    assert strip_scheme("dbfs://") == "/"
    assert strip_scheme("/") == "/"

    with pytest.raises(ValueError):
        strip_scheme("relative/path")

    assert unstrip_scheme("/a/b") == "dbfs:/a/b"
    assert unstrip_scheme("dbfs:/a/b") in "dbfs:/a/b"

    with pytest.raises(ValueError):
        unstrip_scheme("relative/path")

    with pytest.raises(ValueError):
        unstrip_scheme("dbfs:///a/b")


def test_fs_type():
    assert fs_type("dbfs:/Volumes/cat/schema/vol/path") == FileSystemType.Volume
    assert fs_type("/Volumes/cat/schema/vol/path") == FileSystemType.Volume

    assert fs_type("dbfs:/Workspace/Users/foo") == FileSystemType.Workspace
    assert fs_type("/Workspace") == FileSystemType.Workspace

    assert fs_type("dbfs:/some/legacy/path") == FileSystemType.Legacy
    assert fs_type("/some/legacy/path") == FileSystemType.Legacy

    with pytest.raises(ValueError):
        fs_type("not/absolute")


def test_parse_volume_path():
    assert parse_volume_path("dbfs:/Volumes/cat1/schema2/volume3/path/to/file/") == (
        "cat1",
        "schema2",
        "volume3",
        "/path/to/file/",
        "/Volumes/cat1/schema2/volume3/path/to/file/",
    )

    assert parse_volume_path("/Volumes/cat1/schema2/volume3/path/to/file/") == (
        "cat1",
        "schema2",
        "volume3",
        "/path/to/file/",
        "/Volumes/cat1/schema2/volume3/path/to/file/",
    )

    assert parse_volume_path("dbfs:/Volumes/cat1/schema2/volume3/path/to/file") == (
        "cat1",
        "schema2",
        "volume3",
        "/path/to/file",
        "/Volumes/cat1/schema2/volume3/path/to/file",
    )

    assert parse_volume_path("dbfs:/Volumes/cat1/schema2/volume3/") == (
        "cat1",
        "schema2",
        "volume3",
        "/",
        "/Volumes/cat1/schema2/volume3/",
    )

    assert parse_volume_path("dbfs:/Volumes/cat1/schema2/volume3") == (
        "cat1",
        "schema2",
        "volume3",
        "/",
        "/Volumes/cat1/schema2/volume3",
    )

    assert parse_volume_path("dbfs:/Volumes/cat1/schema2") == (
        "cat1",
        "schema2",
        None,
        None,
        "/Volumes/cat1/schema2",
    )

    assert parse_volume_path("dbfs:/Volumes/cat1") == (
        "cat1",
        None,
        None,
        None,
        "/Volumes/cat1",
    )

    assert parse_volume_path("dbfs:/Volumes") == (
        None,
        None,
        None,
        None,
        "/Volumes",
    )

    with pytest.raises(ValueError):
        parse_volume_path("dbfs:/NotVolumes/cat/schema/vol/path")


def test_parse_workspace():
    assert parse_workspace_path("dbfs:/Workspace/Users/me") == "/Users/me"
    assert parse_workspace_path("/Workspace") == "/"

    with pytest.raises(ValueError):
        parse_workspace_path("/NotWorkspace/path")


def test_parse_legacy_dbfs_path():
    assert parse_dbfs_path("dbfs:/path/to/file") == "/path/to/file"
    assert parse_dbfs_path("/path/to/file") == "/path/to/file"

    with pytest.raises(ValueError):
        parse_dbfs_path("relative/path")


def test_to_and_from_local_path():
    # Unity Catalog Volume and Workspace paths are the same locally
    for path in ["/Volumes/c/s/v/path/to/file", "/Workspace/Users/a/file"]:
        assert to_local_path(f"dbfs:{path}", "/dbfs") == path
        assert to_local_path(path, "/dbfs") == path
        assert from_local_path(path, "/dbfs") == path

    # Legacy DBFS paths are under the mount point
    assert to_local_path("dbfs:/data/file", "/dbfs") == "/dbfs/data/file"
    assert to_local_path("/data/file", "/dbfs") == "/dbfs/data/file"
    assert to_local_path("/data/", "/dbfs/") == "/dbfs/data"
    assert to_local_path("dbfs:/", "/dbfs") == "/dbfs"

    assert from_local_path("/dbfs/data/file", "/dbfs") == "/data/file"
    assert from_local_path("/dbfs", "/dbfs/") == "/"
    assert from_local_path("/dbfsx/file", "/dbfs") == "/dbfsx/file"

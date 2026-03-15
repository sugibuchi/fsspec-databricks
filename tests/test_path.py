import pytest

from fsspec_databricks.path import (
    FileSystemType,
    fs_type,
    parse_dbfs_path,
    parse_volume_path,
    parse_workspace_path,
    strip_scheme,
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

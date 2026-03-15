from fsspec.implementations.local import LocalFileSystem

from fsspec_databricks.path import scheme

from .path import strip_scheme, unstrip_scheme


class DatabricksLocalFileSystem(LocalFileSystem):
    """File system accessing resources referenced by DBFS URLs or POSIX paths as ones in the local file system."""

    protocol = scheme
    root_marker = "/"

    @classmethod
    def _strip_protocol(cls, path):
        return strip_scheme(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)


__all__ = [
    "DatabricksLocalFileSystem",
]

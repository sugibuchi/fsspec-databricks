from fsspec.implementations.local import LocalFileSystem

from .path import scheme, strip_scheme, unstrip_scheme


class DatabricksLocalFileSystem(LocalFileSystem):
    """The local file system, taking `dbfs:/` URLs as well as POSIX paths.

    `DatabricksFileSystem` passes local paths, with legacy DBFS paths under their mount point.
    """

    protocol = scheme
    root_marker = "/"

    @classmethod
    def _strip_protocol(cls, path):
        path = str(path)
        if path.startswith(f"{scheme}:"):
            return strip_scheme(path)
        else:
            return super()._strip_protocol(path)

    def unstrip_protocol(self, name: str) -> str:
        return unstrip_scheme(name)


__all__ = [
    "DatabricksLocalFileSystem",
]

from fsspec.registry import register_implementation

from .spec import DatabricksFileSystem


def use():
    """Register the DatabricksFileSystem implementation with fsspec
    as the default file system implementation for "dbfs" URL scheme.

    This function overrides the registration of `DatabricksFileSystem`
    class bundled with fsspec.
    """
    register_implementation("dbfs", DatabricksFileSystem, clobber=True)


__all__ = [
    "DatabricksFileSystem",
    "use",
]

from fsspec.registry import register_implementation

from .spec import DatabricksFileSystem


def use():
    """Register `DatabricksFileSystem` with fsspec for the "dbfs" protocol.

    It replaces fsspec's own `DatabricksFileSystem`, which supports legacy DBFS only.
    """
    register_implementation("dbfs", DatabricksFileSystem, clobber=True)


__all__ = [
    "DatabricksFileSystem",
    "use",
]

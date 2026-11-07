import asyncio

from fsspec.implementations.local import LocalFileSystem

from ._contrib.strategy.tree.wrapper import SyncTreeWrapper
from .path import scheme, strip_scheme, unstrip_scheme


class DatabricksLocalFileSystem(LocalFileSystem):
    """File system accessing resources referenced by DBFS URLs or POSIX paths as ones in the local file system.

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


class LocalTreeWrapper(SyncTreeWrapper):
    """The local file system as an async `TreeFileSystem`, so that `DatabricksFileSystem` can route to it.

    It adds what the library's wrapper does not pass on: `_mkdir`, `created` and `modified`.
    """

    async def _mkdir(self, path, create_parents=True, **kwargs):
        await asyncio.to_thread(self.fs.mkdir, path, create_parents, **kwargs)

    def created(self, path):
        return self.fs.created(path)

    def modified(self, path):
        return self.fs.modified(path)


__all__ = [
    "DatabricksLocalFileSystem",
    "LocalTreeWrapper",
]

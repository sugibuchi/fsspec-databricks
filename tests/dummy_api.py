"""
FastAPI application that simulates Databricks' Files API, Workspace API and legacy DBFS API, for tests.

The APIs answer as their REST references say, with the SDK's error format: a JSON body of `error_code` and
`message`, from which the SDK picks its exception class. The Volumes' files, the Workspace files and the DBFS root
are separate namespaces, as in Databricks. Behaviour that a reference leaves open is marked as an assumption.
"""

import asyncio
import base64
import hashlib
import json
import logging
import posixpath
import re
import socket
import time
import urllib.parse
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from email.utils import formatdate, parsedate_to_datetime
from time import sleep
from typing import Annotated, Literal

import requests
from fastapi import FastAPI, Header, Request, Response
from fastapi.params import Depends
from fastapi.responses import JSONResponse
from hypercorn.asyncio import serve
from hypercorn.config import Config

log = logging.getLogger(__name__)

app = FastAPI()


class ApiError(Exception):
    """An error answered in the format of Databricks' REST APIs."""

    def __init__(self, error_code: str, message: str, status_code: int = 400):
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.status_code = status_code


@app.exception_handler(ApiError)
async def _api_error(request: Request, exc: ApiError):
    log.debug("API error: %s %s: %s", exc.status_code, exc.error_code, exc.message)
    return JSONResponse(
        {"error_code": exc.error_code, "message": exc.message},
        status_code=exc.status_code,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _rfc7231(ms: int) -> str:
    return formatdate(ms / 1000, usegmt=True)


def _bool(value) -> bool:
    if isinstance(value, bytes):
        value = value.decode()
    return value is True or str(value).lower() == "true"


def _text(value) -> str | None:
    return value.decode() if isinstance(value, bytes) else value


async def _body(request: Request) -> dict:
    """A request's fields, from a JSON body or a multipart form, as the SDK sends either."""
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if content_type.startswith("multipart/form-data"):
        boundary = re.search(r'boundary="?([^";]+)"?', content_type).group(1).encode()
        fields = {}
        for part in body.split(b"--" + boundary)[1:-1]:
            head, _, data = part[2:].partition(b"\r\n\r\n")
            name = re.search(rb'name="([^"]*)"', head).group(1).decode()
            fields[name] = data[:-2]  # The CRLF before the next boundary.
        return fields
    return json.loads(body) if body else {}


def _absolute(path: str | None) -> str:
    if not path or not path.startswith("/"):
        raise ApiError(
            "INVALID_PARAMETER_VALUE", f"Path ({path}) must be an absolute path."
        )
    return posixpath.normpath(path).replace("//", "/")


def _subtree(paths, path: str) -> list[str]:
    prefix = path.rstrip("/") + "/"
    return [p for p in paths if p == path or p.startswith(prefix)]


def _children(paths, path: str) -> list[str]:
    return [p for p in paths if p != "/" and posixpath.dirname(p) == path]


### State


@dataclass
class VolumeFile:
    data: bytes
    last_modified: int

    def headers(self) -> dict[str, str]:
        """The headers of a download or a metadata request, as the Files API answers."""
        return {
            "content-length": str(len(self.data)),
            "content-type": "application/octet-stream",
            "last-modified": _rfc7231(self.last_modified),
        }


@dataclass
class MultipartUpload:
    path: str
    overwrite: bool
    parts: dict[int, tuple[str, bytes]] = field(default_factory=dict)
    """The parts by number, each with its ETag."""


@dataclass
class ResumableUpload:
    path: str
    overwrite: bool
    data: bytearray = field(default_factory=bytearray)


@dataclass
class WorkspaceObject:
    object_type: Literal["DIRECTORY", "FILE", "NOTEBOOK"]
    object_id: int
    created_at: int | None = None
    modified_at: int | None = None
    language: str | None = None
    content: bytes = b""

    def info(self, path: str) -> dict:
        info = {
            "path": path,
            "object_type": self.object_type,
            "object_id": self.object_id,
            "resource_id": str(self.object_id),
        }
        # Directories tell no times, notebooks no size: as the Workspace API answers.
        if self.created_at is not None:
            info["created_at"] = self.created_at
            info["modified_at"] = self.modified_at
        if self.object_type == "FILE":
            info["size"] = len(self.content)
        if self.language:
            info["language"] = self.language
        return info


@dataclass
class DbfsEntry:
    is_dir: bool
    modification_time: int
    data: bytearray = field(default_factory=bytearray)

    def info(self, path: str) -> dict:
        return {
            "path": path,
            "is_dir": self.is_dir,
            "file_size": 0 if self.is_dir else len(self.data),
            "modification_time": self.modification_time,
        }


_volume_pat = re.compile(r"/Volumes/[^/]+/[^/]+/[^/]+")


def _volume_root(path: str) -> str:
    return "/".join(path.split("/")[:5])


def _volume_ancestors(path: str) -> list[str]:
    """The directories above a path in its volume, from the top, without the volume itself."""
    root = _volume_root(path)
    names = path[len(root) :].split("/")[1:-1]
    return [root + "/" + "/".join(names[: i + 1]) for i in range(len(names))]


@dataclass
class DummyStorage:
    """The Volumes' files, the Workspace files and the DBFS root of a dummy server, with its test switches."""

    volume_files: dict[str, VolumeFile] = field(default_factory=dict)
    volume_dirs: set[str] = field(default_factory=set)
    multipart_uploads: dict[str, MultipartUpload] = field(default_factory=dict)
    resumable_uploads: dict[str, ResumableUpload] = field(default_factory=dict)

    upload_mode: Literal["multipart", "resumable", "unknown"] = "multipart"
    """The upload API that initiate-upload answers with, as the server chooses it. `unknown` names another one."""

    presigned_urls_only: bool = False
    """Whether the Files API refuses to transfer data itself, so that only presigned URLs do."""

    abort_supported: bool = True
    """Whether multipart uploads can be aborted by a presigned URL, which Azure Databricks refuses."""

    storage_proxy_available: bool = False
    """Whether the storage proxy's probe succeeds, for a client whose proxy host is this server."""

    read_errors: list[int] = field(default_factory=list)
    """Statuses that the next downloads get, one each."""

    workspace: dict[str, WorkspaceObject] = field(default_factory=dict)
    dbfs: dict[str, DbfsEntry] = field(default_factory=dict)
    dbfs_handles: dict[int, str] = field(default_factory=dict)
    legacy_dbfs_disabled: bool = False
    next_id: int = 1

    workspace_id: int = 1234567890

    def __post_init__(self):
        self.reset()

    def reset(self):
        self.volume_files = {}
        self.volume_dirs = set()
        self.multipart_uploads = {}
        self.resumable_uploads = {}
        self.upload_mode = "multipart"
        self.presigned_urls_only = False
        self.abort_supported = True
        self.storage_proxy_available = False
        self.read_errors = []
        self.workspace = {"/": WorkspaceObject("DIRECTORY", 0)}
        self.dbfs = {"/": DbfsEntry(True, 0)}
        self.dbfs_handles = {}
        self.legacy_dbfs_disabled = False

    def new_id(self) -> int:
        self.next_id += 1
        return self.next_id

    ### Files API
    def vol_is_dir(self, path: str) -> bool:
        # Assumed: every volume exists, as the dummy has no Unity Catalog.
        return path in self.volume_dirs or path == _volume_root(path)

    def vol_get(self, path: str) -> VolumeFile:
        if path not in self.volume_files:
            raise ApiError("NOT_FOUND", f"The file {path} is not found.", 404)
        return self.volume_files[path]

    def vol_children(self, path: str) -> list[str]:
        # Assumed: sorted by name, as object storage lists.
        return sorted(_children([*self.volume_files, *self.volume_dirs], path))

    def vol_mkdirs(self, path: str) -> None:
        for p in [*_volume_ancestors(path), path]:
            if p in self.volume_files:
                # Assumed: as for a file at the path itself.
                raise ApiError(
                    "ALREADY_EXISTS", f"Cannot create {path}: {p} is a file.", 409
                )
            if p != _volume_root(p):
                self.volume_dirs.add(p)

    def vol_check_writable(self, path: str, overwrite: bool) -> None:
        if self.vol_is_dir(path):
            # Assumed: a directory is never overwritten by a file.
            raise ApiError("ALREADY_EXISTS", f"A directory exists at {path}.", 409)
        if path in self.volume_files and not overwrite:
            raise ApiError("ALREADY_EXISTS", f"The file {path} already exists.", 409)
        for p in _volume_ancestors(path):
            if p in self.volume_files:
                raise ApiError(
                    "ALREADY_EXISTS", f"Cannot create {path}: {p} is a file.", 409
                )

    def vol_write(self, path: str, data: bytes, overwrite: bool = True) -> None:
        """Write a file, creating its parents: tests set files up with it too."""
        self.vol_check_writable(path, overwrite)
        # Assumed: an upload creates the missing parent directories, as object storage has none to create.
        self.vol_mkdirs(posixpath.dirname(path))
        self.volume_files[path] = VolumeFile(bytes(data), _now_ms())

    def vol_initiate_upload(self, path: str, overwrite: bool) -> dict:
        self.vol_check_writable(path, overwrite)
        token = str(uuid.uuid4())
        if self.upload_mode == "multipart":
            self.multipart_uploads[token] = MultipartUpload(path, overwrite)
            return {"multipart_upload": {"session_token": token}}
        if self.upload_mode == "resumable":
            self.resumable_uploads[token] = ResumableUpload(path, overwrite)
            return {"resumable_upload": {"session_token": token}}
        # An upload API that no client knows.
        return {"other_upload": {"session_token": token}}

    def vol_upload(self, kind: str, token: str | None, path: str):
        uploads = (
            self.multipart_uploads if kind == "multipart" else self.resumable_uploads
        )
        upload = uploads.get(token)
        if upload is None or upload.path != path:
            # Assumed: an unknown session, or one of another file, is not found.
            raise ApiError("NOT_FOUND", f"No {kind} upload session {token}.", 404)
        return upload

    ### Workspace
    def ws_get(self, path: str) -> WorkspaceObject:
        if path not in self.workspace:
            raise ApiError(
                "RESOURCE_DOES_NOT_EXIST", f"Path ({path}) doesn't exist.", 404
            )
        return self.workspace[path]

    def ws_mkdirs(self, path: str) -> None:
        parts = [p for p in path.split("/") if p]
        for i in range(1, len(parts) + 1):
            p = "/" + "/".join(parts[:i])
            if p not in self.workspace:
                self.workspace[p] = WorkspaceObject("DIRECTORY", self.new_id())
            elif self.workspace[p].object_type != "DIRECTORY":
                raise ApiError(
                    "RESOURCE_ALREADY_EXISTS",
                    f"Cannot create directory {path}: {p} is not a directory.",
                )

    def ws_import(
        self,
        path: str,
        object_type: str,
        language: str | None,
        content: bytes,
        overwrite: bool,
    ) -> None:
        parent = posixpath.dirname(path)
        if self.workspace.get(parent, WorkspaceObject("FILE", -1)).object_type != (
            "DIRECTORY"
        ):
            # Assumed: the parents are not created, unlike mkdirs.
            raise ApiError(
                "RESOURCE_DOES_NOT_EXIST",
                f"The parent folder ({parent}) does not exist.",
                404,
            )
        now = _now_ms()
        existing = self.workspace.get(path)
        if existing is None:
            self.workspace[path] = WorkspaceObject(
                object_type, self.new_id(), now, now, language, bytes(content)
            )
            return
        if not overwrite or existing.object_type == "DIRECTORY":
            raise ApiError("RESOURCE_ALREADY_EXISTS", f"Path ({path}) already exists.")
        # Overwriting keeps the object, its place in listings and its creation time.
        existing.object_type = object_type
        existing.language = language
        existing.content = bytes(content)
        existing.modified_at = now

    ### DBFS
    def dbfs_get(self, path: str) -> DbfsEntry:
        if path not in self.dbfs:
            raise ApiError(
                "RESOURCE_DOES_NOT_EXIST",
                f"No file or directory exists on path {path}.",
                404,
            )
        return self.dbfs[path]

    def dbfs_mkdirs(self, path: str) -> None:
        parts = [p for p in path.split("/") if p]
        for i in range(1, len(parts) + 1):
            p = "/" + "/".join(parts[:i])
            if p not in self.dbfs:
                self.dbfs[p] = DbfsEntry(True, _now_ms())
            elif not self.dbfs[p].is_dir:
                raise ApiError(
                    "RESOURCE_ALREADY_EXISTS",
                    f"A file or directory already exists at the input path {p}.",
                )

    def dbfs_create(self, path: str, overwrite: bool) -> DbfsEntry:
        # Assumed: overwrite replaces a file, never a directory.
        if path in self.dbfs and (not overwrite or self.dbfs[path].is_dir):
            raise ApiError(
                "RESOURCE_ALREADY_EXISTS",
                f"A file or directory already exists at the input path {path}.",
            )
        # Assumed: the parents are created, as `mkdirs` does.
        self.dbfs_mkdirs(posixpath.dirname(path))
        entry = DbfsEntry(False, _now_ms())
        self.dbfs[path] = entry
        return entry


_storage = DummyStorage()


def dummy_storage() -> DummyStorage:
    return _storage


Storage = Annotated[DummyStorage, Depends(dummy_storage)]


@app.get("/_headers")
async def get_headers(request: Request):
    return dict(request.headers)


@app.get("/api/2.0/preview/scim/v2/Me")
async def scim_me(storage: Storage):
    # The SDK reads the workspace ID from this header, e.g. before probing the storage proxy.
    return JSONResponse(
        {"id": "1", "userName": "dummy@example.com"},
        headers={"X-Databricks-Org-Id": str(storage.workspace_id)},
    )


### Files API
#
# Files and directories in Unity Catalog volumes, under `/Volumes/<catalog>/<schema>/<volume>`. Uploads in parts
# go through the upload API that initiate-upload names: multipart, as on AWS and Azure, or resumable, as on GCP.
# Their parts go to presigned URLs, or to the Files API itself with the session in the query, as the storage proxy
# takes them. The presigned URLs point back to this server, with `signed=true`.

_max_page_size = 1000
_resumable_granularity = 256 * 1024
_probe_path = "/DatabricksInternal/Probe/fullstack/wis"


def _volume_path(path: str | None) -> str:
    path = _absolute(path)
    if not _volume_pat.match(path):
        # Assumed: the error of a path outside the volumes.
        raise ApiError(
            "INVALID_PARAMETER_VALUE",
            f"Path ({path}) is not in a Unity Catalog volume.",
        )
    return path


def _overwrite(value: str | None) -> bool:
    # The reference: an existing file is overwritten when `overwrite` is true or unspecified.
    return value is None or _bool(value)


def _check_presigned(storage: DummyStorage, signed: str | None) -> None:
    if storage.presigned_urls_only and signed != "true":
        raise ApiError("PERMISSION_DENIED", "Only presigned URLs transfer data.", 403)


def _presigned_url(request: Request, path: str, **query) -> str:
    base = str(request.base_url).rstrip("/")
    query = urllib.parse.urlencode({**query, "signed": "true"})
    return f"{base}/api/2.0/fs/files{urllib.parse.quote(path)}?{query}"


_range_pat = re.compile(r"bytes=(?P<first>\d*)-(?P<last>\d*)")


def _byte_range(header: str, size: int) -> tuple[int, int] | None:
    """The first and last positions of a Range header, or None for a range past the end."""
    m = _range_pat.fullmatch(header)
    if not m or not (m["first"] or m["last"]):
        # Assumed: a malformed range is refused, not ignored.
        raise ApiError("INVALID_PARAMETER_VALUE", f"Invalid Range header: {header}")
    if not m["first"]:
        # A suffix: the last bytes.
        return max(size - int(m["last"]), 0), size - 1
    first = int(m["first"])
    last = min(int(m["last"]), size - 1) if m["last"] else size - 1
    if first >= size or first > last:
        return None
    return first, last


@app.get("/api/2.0/fs/files{file_path:path}")
async def files_download(
    storage: Storage,
    file_path: str,
    range: Annotated[str | None, Header()] = None,
    if_unmodified_since: Annotated[str | None, Header()] = None,
    signed: str | None = None,
):
    if storage.read_errors:
        raise ApiError("INTERNAL_ERROR", "Injected error", storage.read_errors.pop(0))
    _check_presigned(storage, signed)
    path = _volume_path(file_path)
    file = storage.vol_get(path)

    # The SDK resumes a broken download from where it stopped, if the file did not change.
    if (
        if_unmodified_since
        and parsedate_to_datetime(if_unmodified_since).timestamp()
        < file.last_modified // 1000
    ):
        raise ApiError("FAILED_PRECONDITION", f"The file {path} was modified.", 412)

    headers = file.headers()
    if not range:
        return Response(content=file.data, headers=headers)
    positions = _byte_range(range, len(file.data))
    if positions is None:
        return JSONResponse(
            {"error_code": "INVALID_PARAMETER_VALUE", "message": "Invalid range."},
            status_code=416,
            headers={"content-range": f"bytes */{len(file.data)}"},
        )
    first, last = positions
    headers["content-range"] = f"bytes {first}-{last}/{len(file.data)}"
    headers["content-length"] = str(last - first + 1)
    return Response(
        content=file.data[first : last + 1], headers=headers, status_code=206
    )


@app.head("/api/2.0/fs/files{file_path:path}")
async def files_get_metadata(storage: Storage, file_path: str):
    if file_path == _probe_path:
        # The storage proxy's probe: 200 when it serves the workspace.
        return Response(status_code=200 if storage.storage_proxy_available else 403)
    path = _volume_path(file_path)
    # A HEAD response has no body: the SDK tells the error by its status alone.
    return Response(headers=storage.vol_get(path).headers())


@app.put("/api/2.0/fs/files{file_path:path}")
async def files_upload(
    storage: Storage,
    file_path: str,
    request: Request,
    overwrite: str | None = None,
    upload_type: str | None = None,
    session_token: str | None = None,
    part_number: int | None = None,
    content_range: Annotated[str | None, Header()] = None,
    signed: str | None = None,
):
    path = _volume_path(file_path)
    _check_presigned(storage, signed)
    data = await request.body()

    if upload_type == "multipart":
        upload = storage.vol_upload("multipart", session_token, path)
        if part_number is None or part_number < 1:
            raise ApiError("INVALID_PARAMETER_VALUE", f"Invalid part: {part_number}")
        # Uploading a part again replaces it, as object storage does.
        etag = base64.b64encode(hashlib.sha256(data).digest()).decode()
        upload.parts[part_number] = (etag, data)
        return Response(headers={"etag": etag})
    if upload_type == "resumable":
        return _resumable_put(storage, path, session_token, content_range, data)
    if upload_type is not None:
        raise ApiError("INVALID_PARAMETER_VALUE", f"Invalid upload_type: {upload_type}")

    storage.vol_write(path, data, _overwrite(overwrite))
    return Response(status_code=204)


_content_range_pat = re.compile(
    r"bytes (?:(?P<first>\d+)-(?P<last>\d+)|(?P<status>\*))/(?P<total>\d+|\*)"
)


def _resumable_put(
    storage: DummyStorage,
    path: str,
    session_token: str | None,
    content_range: str | None,
    data: bytes,
) -> Response:
    """A chunk of a resumable upload, or a query of its status, as Google Cloud Storage takes them."""
    upload = storage.vol_upload("resumable", session_token, path)
    m = _content_range_pat.fullmatch(content_range or "")
    if not m:
        raise ApiError(
            "INVALID_PARAMETER_VALUE", f"Invalid Content-Range: {content_range}"
        )

    if not m["status"]:
        first, last = int(m["first"]), int(m["last"])
        total = None if m["total"] == "*" else int(m["total"])
        if first > len(upload.data) or len(data) != last - first + 1:
            # Assumed: a gap or a wrong length is refused.
            raise ApiError(
                "INVALID_PARAMETER_VALUE", f"Unexpected chunk: {content_range}"
            )
        if total is None and len(data) % _resumable_granularity != 0:
            raise ApiError(
                "INVALID_PARAMETER_VALUE",
                f"A chunk but the last must be a multiple of 256 KiB: {content_range}",
            )
        # A chunk overlapping what was received replaces it.
        upload.data[first:] = data
        if total is not None:
            if len(upload.data) != total:
                raise ApiError(
                    "INVALID_PARAMETER_VALUE", f"Unexpected size: {content_range}"
                )
            del storage.resumable_uploads[session_token]
            if path in storage.volume_files and not upload.overwrite:
                # As Cloud Storage answers a failed precondition, which the SDK expects.
                raise ApiError(
                    "ALREADY_EXISTS", f"The file {path} already exists.", 412
                )
            storage.vol_write(path, upload.data, upload.overwrite)
            return Response(status_code=200)

    # 308 tells what was received so far, and nothing before the first byte.
    headers = {"range": f"bytes=0-{len(upload.data) - 1}"} if upload.data else {}
    return Response(status_code=308, headers=headers)


@app.post("/api/2.0/fs/files{file_path:path}")
async def files_upload_action(
    storage: Storage,
    file_path: str,
    request: Request,
    action: str | None = None,
    upload_type: str | None = None,
    session_token: str | None = None,
    overwrite: str | None = None,
):
    path = _volume_path(file_path)
    if action == "initiate-upload":
        return storage.vol_initiate_upload(path, _overwrite(overwrite))
    if action == "complete-upload" and upload_type == "multipart":
        upload = storage.vol_upload("multipart", session_token, path)
        parts = (await _body(request)).get("parts") or []
        numbers = [p["part_number"] for p in parts]
        if not numbers or numbers != sorted(set(numbers)):
            raise ApiError(
                "INVALID_PARAMETER_VALUE", "The parts must be in ascending order."
            )
        data = bytearray()
        for p in parts:
            etag, part = upload.parts.get(p["part_number"], (None, b""))
            if etag is None or etag != p["etag"]:
                raise ApiError("INVALID_PARAMETER_VALUE", f"Unknown part or ETag: {p}")
            data.extend(part)
        del storage.multipart_uploads[session_token]
        storage.vol_write(path, data, upload.overwrite)
        return {}
    if action == "abort-upload" and upload_type == "multipart":
        # The SDK aborts with a POST, fsspec-databricks with a DELETE: both are served.
        storage.vol_upload("multipart", session_token, path)
        del storage.multipart_uploads[session_token]
        return {}
    raise ApiError(
        "INVALID_PARAMETER_VALUE",
        f"Invalid action: action={action}, upload_type={upload_type}",
    )


@app.delete("/api/2.0/fs/files{file_path:path}")
async def files_delete(
    storage: Storage,
    file_path: str,
    action: str | None = None,
    upload_type: str | None = None,
    session_token: str | None = None,
    signed: str | None = None,
):
    path = _volume_path(file_path)
    if action == "abort-upload":
        _check_presigned(storage, signed)
        storage.vol_upload("multipart", session_token, path)
        del storage.multipart_uploads[session_token]
        return Response(status_code=204)
    if upload_type == "resumable":
        # Deleting the session's URL cancels it, as on Cloud Storage.
        _check_presigned(storage, signed)
        storage.vol_upload("resumable", session_token, path)
        del storage.resumable_uploads[session_token]
        return Response(status_code=204)
    if action or upload_type or session_token:
        raise ApiError(
            "INVALID_PARAMETER_VALUE",
            f"Invalid action: action={action}, upload_type={upload_type}",
        )
    # Assumed: a directory is no file, so it is not found.
    storage.vol_get(path)
    del storage.volume_files[path]
    return Response(status_code=204)


@app.put("/api/2.0/fs/directories{directory_path:path}")
async def files_create_directory(storage: Storage, directory_path: str):
    # The reference: the parents are created, and an existing directory is no error.
    storage.vol_mkdirs(_volume_path(directory_path))
    return Response(status_code=204)


@app.delete("/api/2.0/fs/directories{directory_path:path}")
async def files_delete_directory(storage: Storage, directory_path: str):
    path = _volume_path(directory_path)
    if path == _volume_root(path):
        # Assumed: a volume is deleted by the Unity Catalog API only.
        raise ApiError("INVALID_PARAMETER_VALUE", f"Cannot delete the volume {path}.")
    if path not in storage.volume_dirs:
        raise ApiError("NOT_FOUND", f"The directory {path} is not found.", 404)
    if storage.vol_children(path):
        # The reference: only an empty directory is deleted. Assumed: the error's code and message.
        raise ApiError("DIRECTORY_NOT_EMPTY", f"Directory {path} is not empty.")
    storage.volume_dirs.remove(path)
    return Response(status_code=204)


@app.head("/api/2.0/fs/directories{directory_path:path}")
async def files_get_directory_metadata(storage: Storage, directory_path: str):
    path = _volume_path(directory_path)
    # Assumed: a file is no directory, so it is not found.
    return Response(status_code=200 if storage.vol_is_dir(path) else 404)


@app.get("/api/2.0/fs/directories{directory_path:path}")
async def files_list_directory_contents(
    storage: Storage,
    directory_path: str,
    page_size: int | None = None,
    page_token: str | None = None,
):
    path = _volume_path(directory_path)
    if not storage.vol_is_dir(path):
        # Assumed: listing a file fails as listing a missing directory.
        raise ApiError("NOT_FOUND", f"The directory {path} is not found.", 404)
    # The reference: at most 1000 entries a page, larger sizes taken as 1000.
    size = min(page_size, _max_page_size) if page_size and page_size > 0 else 1000

    children = storage.vol_children(path)
    if page_token:
        # Assumed: the token is opaque; here, the last path of the previous page.
        try:
            after = base64.b64decode(page_token, b"-_", validate=True).decode()
        except ValueError:
            raise ApiError(
                "INVALID_PARAMETER_VALUE", f"Invalid page token: {page_token}"
            ) from None
        children = [p for p in children if p > after]

    page = children[:size]
    response = {}
    if page:
        response["contents"] = [_directory_entry(storage, p) for p in page]
    if len(children) > size:
        response["next_page_token"] = base64.urlsafe_b64encode(
            page[-1].encode()
        ).decode()
    return response


def _directory_entry(storage: DummyStorage, path: str) -> dict:
    entry = {"path": path, "name": posixpath.basename(path)}
    file = storage.volume_files.get(path)
    if file is None:
        # Assumed: directories tell no size and no time, as object storage keeps none.
        entry["is_directory"] = True
    else:
        entry["is_directory"] = False
        entry["file_size"] = len(file.data)
        entry["last_modified"] = file.last_modified
    return entry


@app.post("/api/2.0/fs/create-download-url")
async def files_create_download_url(
    storage: Storage,
    request: Request,
    path: str | None = None,
    expire_time: str | None = None,
):
    if expire_time is None:
        raise ApiError("INVALID_PARAMETER_VALUE", "expire_time is required.")
    path = _volume_path(path)
    # Assumed: a URL for a missing file is refused.
    storage.vol_get(path)
    return {"url": _presigned_url(request, path), "headers": []}


async def _session_request(storage: DummyStorage, request: Request, kind: str):
    """The path and session token of a request for upload URLs, checked."""
    body = await _body(request)
    path = _volume_path(body.get("path"))
    token = body.get("session_token")
    storage.vol_upload(kind, token, path)
    return body, path, token


@app.post("/api/2.0/fs/create-upload-part-urls")
async def files_create_upload_part_urls(storage: Storage, request: Request):
    body, path, token = await _session_request(storage, request, "multipart")
    try:
        start, count = int(body["start_part_number"]), int(body["count"])
        expire_time = body["expire_time"]
    except (KeyError, TypeError, ValueError) as e:
        raise ApiError("INVALID_PARAMETER_VALUE", f"Invalid request: {e}") from e
    return {
        "upload_part_urls": [
            {
                "url": _presigned_url(
                    request,
                    path,
                    session_token=token,
                    upload_type="multipart",
                    part_number=i,
                ),
                "headers": [],
                "part_number": i,
                "expire_time": expire_time,
            }
            for i in range(start, start + count)
        ]
    }


@app.post("/api/2.0/fs/create-resumable-upload-url")
async def files_create_resumable_upload_url(storage: Storage, request: Request):
    _, path, token = await _session_request(storage, request, "resumable")
    url = _presigned_url(request, path, session_token=token, upload_type="resumable")
    return {"resumable_upload_url": {"url": url, "headers": []}}


@app.post("/api/2.0/fs/create-abort-upload-url")
async def files_create_abort_upload_url(storage: Storage, request: Request):
    if not storage.abort_supported:
        # As Azure Databricks answers, which cannot abort multipart uploads.
        raise ApiError(
            "INVALID_PARAMETER_VALUE", "Abort of multipart upload is not supported."
        )
    body, path, token = await _session_request(storage, request, "multipart")
    if "expire_time" not in body:
        raise ApiError("INVALID_PARAMETER_VALUE", "expire_time is required.")
    url = _presigned_url(request, path, action="abort-upload", session_token=token)
    return {
        "abort_upload_url": {
            "url": url,
            "headers": [],
            "expire_time": body["expire_time"],
        }
    }


### Workspace API

# Notebook extensions by language, which an AUTO import strips, with the header of each language's source.
_notebook_extensions = {
    ".py": ("PYTHON", b"# Databricks notebook source"),
    ".sql": ("SQL", b"-- Databricks notebook source"),
    ".scala": ("SCALA", b"// Databricks notebook source"),
    ".r": ("R", b"# Databricks notebook source"),
}
_workspace_max_size = 10 * 1024 * 1024


@app.get("/api/2.0/workspace/get-status")
async def workspace_get_status(storage: Storage, path: str | None = None):
    path = _absolute(path)
    return storage.ws_get(path).info(path)


@app.get("/api/2.0/workspace/list")
async def workspace_list(storage: Storage, path: str | None = None):
    path = _absolute(path)
    obj = storage.ws_get(path)
    if obj.object_type != "DIRECTORY":
        return {"objects": [obj.info(path)]}
    # In creation order, as the Workspace API lists.
    children = _children(storage.workspace, path)
    if not children:
        return {}
    return {"objects": [storage.workspace[p].info(p) for p in children]}


@app.post("/api/2.0/workspace/mkdirs")
async def workspace_mkdirs(storage: Storage, request: Request):
    storage.ws_mkdirs(_absolute((await _body(request)).get("path")))
    return {}


@app.post("/api/2.0/workspace/delete")
async def workspace_delete(storage: Storage, request: Request):
    body = await _body(request)
    path = _absolute(body.get("path"))
    storage.ws_get(path)
    if _children(storage.workspace, path) and not _bool(body.get("recursive")):
        raise ApiError("DIRECTORY_NOT_EMPTY", f"Folder ({path}) is not empty.")
    for p in _subtree(storage.workspace, path):
        del storage.workspace[p]
    return {}


@app.post("/api/2.0/workspace/import")
async def workspace_import(storage: Storage, request: Request):
    body = await _body(request)
    path = _absolute(_text(body.get("path")))
    fmt = _text(body.get("format")) or "SOURCE"
    language = _text(body.get("language"))
    overwrite = _bool(body.get("overwrite"))
    content = body.get("content") or b""
    # A JSON body has the content in base64, a form has it as a posted file.
    if isinstance(content, str):
        content = base64.b64decode(content)

    if len(content) > _workspace_max_size:
        raise ApiError("MAX_NOTEBOOK_SIZE_EXCEEDED", f"File size exceeds 10 MB: {path}")

    path, object_type, language = _import_kind(path, fmt, language, content)
    storage.ws_import(path, object_type, language, content, overwrite)
    return {}


def _import_kind(
    path: str, fmt: str, language: str | None, content: bytes
) -> tuple[str, str, str | None]:
    """The path, object type and language of an import, from its format."""
    if fmt == "RAW":
        object_type = "FILE"
    elif fmt == "SOURCE":
        if not language:
            # Without a language, SOURCE imports a directory, which the dummy does not support.
            raise ApiError(
                "INVALID_PARAMETER_VALUE",
                "The language must be set to import a notebook as SOURCE.",
            )
        object_type = "NOTEBOOK"
    elif fmt == "AUTO":
        base, ext = posixpath.splitext(path)
        kind = _notebook_extensions.get(ext.lower())
        if kind and content.startswith(kind[1]):
            object_type, language, path = "NOTEBOOK", kind[0], base
        else:
            object_type, language = "FILE", None
    else:
        # HTML, JUPYTER, DBC and R_MARKDOWN convert notebooks: the dummy keeps sources only.
        raise ApiError(
            "INVALID_PARAMETER_VALUE",
            f"Import format not supported by the dummy: {fmt}",
        )
    if object_type == "FILE":
        language = None
    return path, object_type, language


@app.get("/api/2.0/workspace/export")
async def workspace_export(
    storage: Storage,
    path: str | None = None,
    format: str = "SOURCE",
    direct_download: str | None = None,
):
    path = _absolute(path)
    obj = storage.ws_get(path)
    if obj.object_type == "DIRECTORY":
        # Directories export as archives, which the dummy does not make.
        raise ApiError(
            "INVALID_PARAMETER_VALUE", f"Exporting directories is not supported: {path}"
        )
    if obj.object_type == "NOTEBOOK" and format not in ("SOURCE", "AUTO"):
        raise ApiError(
            "INVALID_PARAMETER_VALUE",
            f"Export format not supported by the dummy: {format}",
        )
    if len(obj.content) > _workspace_max_size:
        raise ApiError("MAX_NOTEBOOK_SIZE_EXCEEDED", f"File size exceeds 10 MB: {path}")

    # Files export as they are, in any format: assumed, as the reference names only notebook formats.
    if _bool(direct_download):
        return Response(content=obj.content, media_type="application/octet-stream")
    file_type = None
    if obj.object_type == "NOTEBOOK":
        file_type = {"PYTHON": "py", "SQL": "sql", "SCALA": "scala", "R": "r"}.get(
            obj.language
        )
    response = {"content": base64.b64encode(obj.content).decode()}
    if file_type:
        response["file_type"] = file_type
    return response


@app.get("/api/2.1/settings/{name}")
async def get_setting(storage: Storage, name: str):
    if name != "disable_legacy_dbfs":
        raise ApiError("RESOURCE_DOES_NOT_EXIST", f"Setting {name} not found.", 404)
    value = {"value": storage.legacy_dbfs_disabled}
    return {"name": name, "boolean_val": value, "effective_boolean_val": value}


### Legacy DBFS API
_dbfs_max_block_size = 1024 * 1024


def _dbfs_path(path) -> str:
    path = _text(path)
    if path and path.startswith("dbfs:"):
        path = path[len("dbfs:") :]
    return _absolute(path)


def _check_dbfs_enabled(storage: DummyStorage) -> None:
    if storage.legacy_dbfs_disabled:
        raise ApiError("PERMISSION_DENIED", "Public DBFS root is disabled.", 403)


@app.get("/api/2.0/dbfs/get-status")
async def dbfs_get_status(storage: Storage, path: str | None = None):
    _check_dbfs_enabled(storage)
    path = _dbfs_path(path)
    return storage.dbfs_get(path).info(path)


@app.get("/api/2.0/dbfs/list")
async def dbfs_list(storage: Storage, path: str | None = None):
    _check_dbfs_enabled(storage)
    path = _dbfs_path(path)
    entry = storage.dbfs_get(path)
    if not entry.is_dir:
        return {"files": [entry.info(path)]}
    # Assumed: sorted by name, as the Hadoop file system under DBFS lists.
    children = sorted(_children(storage.dbfs, path))
    if not children:
        return {}
    return {"files": [storage.dbfs[p].info(p) for p in children]}


@app.post("/api/2.0/dbfs/mkdirs")
async def dbfs_mkdirs(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    storage.dbfs_mkdirs(_dbfs_path((await _body(request)).get("path")))
    return {}


@app.post("/api/2.0/dbfs/delete")
async def dbfs_delete(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    body = await _body(request)
    path = _dbfs_path(body.get("path"))
    if path not in storage.dbfs:
        # Assumed: deleting a missing path succeeds, as the reference names no error for it.
        return {}
    if path == "/":
        raise ApiError("INVALID_PARAMETER_VALUE", "Cannot delete the root.")
    if _children(storage.dbfs, path) and not _bool(body.get("recursive")):
        raise ApiError(
            "IO_ERROR",
            f"Directory {path} is not empty: set recursive to delete it.",
        )
    for p in _subtree(storage.dbfs, path):
        del storage.dbfs[p]
    return {}


@app.post("/api/2.0/dbfs/move")
async def dbfs_move(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    body = await _body(request)
    src = _dbfs_path(body.get("source_path"))
    dst = _dbfs_path(body.get("destination_path"))
    storage.dbfs_get(src)
    if dst in storage.dbfs:
        raise ApiError(
            "RESOURCE_ALREADY_EXISTS",
            f"A file or directory already exists at the input path {dst}.",
        )
    if src == "/" or dst.startswith(src.rstrip("/") + "/"):
        raise ApiError("INVALID_PARAMETER_VALUE", f"Cannot move {src} into {dst}.")
    # Assumed: the destination's parents are created, as `mkdirs` does.
    storage.dbfs_mkdirs(posixpath.dirname(dst))
    # A directory moves with all in it.
    for p in _subtree(storage.dbfs, src):
        storage.dbfs[dst + p[len(src) :]] = storage.dbfs.pop(p)
    return {}


@app.post("/api/2.0/dbfs/create")
async def dbfs_create(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    body = await _body(request)
    path = _dbfs_path(body.get("path"))
    # Assumed: the file exists, empty, from its creation on, and grows with each block, as on HDFS.
    storage.dbfs_create(path, _bool(body.get("overwrite")))
    handle = storage.new_id()
    storage.dbfs_handles[handle] = path
    return {"handle": handle}


def _dbfs_handle(storage: DummyStorage, handle) -> str:
    if handle not in storage.dbfs_handles:
        raise ApiError("RESOURCE_DOES_NOT_EXIST", f"Unknown handle: {handle}", 404)
    return storage.dbfs_handles[handle]


def _dbfs_block(data) -> bytes:
    block = base64.b64decode(data) if isinstance(data, str) else bytes(data or b"")
    if len(block) > _dbfs_max_block_size:
        raise ApiError(
            "MAX_BLOCK_SIZE_EXCEEDED",
            f"The block of {len(block)} bytes exceeds the limit of 1 MB.",
        )
    return block


@app.post("/api/2.0/dbfs/add-block")
async def dbfs_add_block(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    body = await _body(request)
    path = _dbfs_handle(storage, body.get("handle"))
    block = _dbfs_block(body.get("data"))
    entry = storage.dbfs.get(path)
    if entry is not None and not entry.is_dir:
        entry.data.extend(block)
        entry.modification_time = _now_ms()
    return {}


@app.post("/api/2.0/dbfs/close")
async def dbfs_close(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    body = await _body(request)
    _dbfs_handle(storage, body.get("handle"))
    del storage.dbfs_handles[body["handle"]]
    return {}


@app.post("/api/2.0/dbfs/put")
async def dbfs_put(storage: Storage, request: Request):
    _check_dbfs_enabled(storage)
    # A JSON body has the contents in base64, limited to 1 MB; a form posts them as a file.
    body = await _body(request)
    path = _dbfs_path(body.get("path"))
    contents = body.get("contents") or body.get("file") or b""
    if isinstance(contents, str):
        contents = _dbfs_block(contents)
    entry = storage.dbfs_create(path, _bool(body.get("overwrite")))
    entry.data.extend(contents)
    return {}


@app.get("/api/2.0/dbfs/read")
async def dbfs_read(
    storage: Storage,
    path: str | None = None,
    offset: int = 0,
    length: int = 512 * 1024,
):
    _check_dbfs_enabled(storage)
    path = _dbfs_path(path)
    entry = storage.dbfs_get(path)
    if entry.is_dir:
        raise ApiError("INVALID_PARAMETER_VALUE", f"Path {path} is a directory.")
    if offset < 0 or length < 0:
        raise ApiError(
            "INVALID_PARAMETER_VALUE", f"Invalid offset or length: {offset}, {length}"
        )
    if length > _dbfs_max_block_size:
        raise ApiError(
            "MAX_READ_SIZE_EXCEEDED",
            f"Cannot read more than 1 MB: {length} bytes requested.",
        )
    data = bytes(entry.data[offset : offset + length])
    return {"bytes_read": len(data), "data": base64.b64encode(data).decode()}


### Server


def _find_free_port(start_port=8000, end_port=9000) -> str:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        for port in range(start_port, end_port):
            try:
                s.bind(("127.0.0.1", port))
                return f"127.0.0.1:{port}"
            except OSError:
                pass
        raise RuntimeError("No free port found in the specified range")


def _wait_for_server(url: str):
    for _ in range(30):
        try:
            res = requests.get(url)
            if res.status_code in (200, 404):
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    raise RuntimeError("Dummy API server did not start within the expected time")


def serve_app(loop):
    config = Config()
    config.bind = [_find_free_port()]
    server_url = f"http://{config.bind[0]}"

    shutdown_event = asyncio.Event()
    future = asyncio.run_coroutine_threadsafe(
        serve(app=app, config=config, shutdown_trigger=shutdown_event.wait),
        loop,
    )
    sleep(1.0)
    try:
        yield server_url
    finally:
        log.info("Closing the dummy API server")
        loop.call_soon_threadsafe(shutdown_event.set)
        try:
            future.result(timeout=5.0)
            log.info("Closed the dummy API server")
        except TimeoutError:
            log.info("Closed the dummy API server with timeout")


__all__ = [
    "DummyStorage",
    "dummy_storage",
    "serve_app",
]

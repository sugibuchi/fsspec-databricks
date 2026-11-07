"""
FastAPI application that simulates Databricks' Files API, Workspace API and legacy DBFS API, for tests.
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
from base64 import b64encode
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import sleep
from typing import Annotated, Literal

import requests
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.params import Depends, Header
from fastapi.responses import JSONResponse
from hypercorn.asyncio import serve
from hypercorn.config import Config
from pydantic import BaseModel

log = logging.getLogger(__name__)


@dataclass
class DummyAPIContext:
    upload_mode: Literal["multipart", "resumable", "unknown"] = "multipart"
    check_signed_url: bool = False
    abort_supported: bool = True

    get_file_errors: list[int] = field(default_factory=list)
    """Statuses that the next requests to read a file get, one each."""

    files: dict[str, bytes] = field(default_factory=dict)

    multipart_upload_sessions: dict[str, dict[tuple[int, str], bytes | None]] = field(
        default_factory=dict
    )

    resumable_upload_sessions: dict[str, tuple[bytes, bool]] = field(
        default_factory=dict
    )

    def check_multipart_upload_session(self, session_token: str):
        if session_token not in self.multipart_upload_sessions:
            raise http_exception(
                "BAD_REQUEST",
                f"Unknown multipart upload session: {session_token}",
                400,
            )

    def check_resumable_upload_session(self, session_token: str):
        if session_token not in self.resumable_upload_sessions:
            raise http_exception(
                "BAD_REQUEST",
                f"Unknown resumable upload session: {session_token}",
                400,
            )


def http_exception(error_code: str, message: str, status_code: int):
    """Create HTTPException object with logging"""
    log.debug(
        "Raising HTTPException: error_code=%s, message=%s, status_code=%d",
        error_code,
        message,
        status_code,
    )

    return HTTPException(
        detail={
            "error_code": error_code,
            "message": message,
        },
        status_code=status_code,
    )


class Part(BaseModel):
    part_number: int
    etag: str


class Parts(BaseModel):
    parts: list[Part]


app = FastAPI()

_dummy_context = DummyAPIContext()


def dummy_context() -> DummyAPIContext:
    return _dummy_context


def _now_rfc7231():
    return datetime.now(tz=UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")


_range_pat = re.compile(r"bytes=(?P<first_pos>\d+)-(?P<last_pos>\d+)")
_resumable_upload_content_range_pat = re.compile(
    r"bytes (?:(?P<first_pos>\d+)-(?P<last_pos>\d+)|(?P<unknown_range>\*))/(?P<total_size>\d+|\*)"
)


@app.get("/_headers")
async def get_headers(request: Request):
    return dict(request.headers)


@app.get("/api/2.0/fs/files{file_path:path}")
async def get_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    range: Annotated[str | None, Header()] = None,
    signed: Annotated[str | None, Query()] = None,
):
    if context.get_file_errors:
        raise http_exception(
            "INTERNAL_ERROR",
            "Injected error",
            context.get_file_errors.pop(0),
        )

    if context.check_signed_url and signed != "true":
        raise http_exception(
            "BAD_REQUEST",
            "Signed URL is required",
            400,
        )

    if file_path not in context.files:
        raise http_exception(
            "NOT_FOUND",
            "File not found",
            404,
        )

    data = context.files[file_path]
    if range:
        m = _range_pat.fullmatch(range)
        if not m:
            raise http_exception(
                "BAD_REQUEST",
                "Invalid Range header",
                400,
            )
        first_pos = int(m.group("first_pos"))
        last_pos = int(m.group("last_pos"))
        return Response(
            status_code=206,
            media_type="application/octet-stream",
            headers={
                "content-range": f"bytes {first_pos}-{last_pos}/{len(data)}",
                "content-length": str(last_pos - first_pos + 1),
                "last-modified": _now_rfc7231(),
            },
            content=data[first_pos : last_pos + 1],
        )
    else:
        return Response(
            status_code=200,
            media_type="application/octet-stream",
            headers={
                "content-range": f"bytes {0}-{len(data)}/{len(data)}",
                "content-length": str(len(data)),
                "last-modified": _now_rfc7231(),
            },
            content=data,
        )


def _initiate_upload(context: DummyAPIContext) -> dict:
    """Start a session of the upload API that the context names, as the server chooses."""
    session_token = str(uuid.uuid4())
    if context.upload_mode == "multipart":
        context.multipart_upload_sessions[session_token] = {}
        return {"multipart_upload": {"session_token": session_token}}
    if context.upload_mode == "resumable":
        context.resumable_upload_sessions[session_token] = (b"", False)
        return {"resumable_upload": {"session_token": session_token}}
    # An upload API this package does not know.
    return {"other_upload": {"session_token": session_token}}


@app.post("/api/2.0/fs/files{file_path:path}")
async def post_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    request: Request,
    action: Annotated[str | None, Query()] = None,
    session_token: Annotated[str | None, Query()] = None,
    upload_type: Annotated[str | None, Query()] = None,
    overwrite: Annotated[str | None, Query()] = None,
):
    if not overwrite and file_path in context.files:
        raise http_exception(
            "RESOURCE_ALREADY_EXISTS",
            f"File already exists: {file_path}",
            409,
        )

    if action is not None:
        if action == "initiate-upload":
            return _initiate_upload(context)

        elif action == "complete-upload":
            if upload_type != "multipart":
                raise http_exception(
                    "BAD_REQUEST",
                    f"Invalid upload_type: {upload_type}",
                    400,
                )

            context.check_multipart_upload_session(session_token)
            session = context.multipart_upload_sessions.pop(session_token)

            parts = sorted(
                [
                    (p.part_number, p.etag)
                    for p in Parts.model_validate(await request.json()).parts
                ]
            )

            data = bytearray()
            for p in sorted(parts):
                if p not in session:
                    raise http_exception(
                        "BAD_REQUEST",
                        f"Invalid part number or etag: {p}",
                        400,
                    )

                if session[p] is None:
                    raise http_exception(
                        "BAD_REQUEST",
                        f"Data for part number {p[0]} is not uploaded",
                        400,
                    )
                data.extend(session[p])
            context.files[file_path] = bytes(data)

            return Response(status_code=206)

    raise http_exception(
        "BAD_REQUEST",
        f"Invalid action: {action}",
        400,
    )


@app.put("/api/2.0/fs/files{file_path:path}")
async def put_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    request: Request,
    session_token: Annotated[str | None, Query()] = None,
    upload_type: Annotated[str | None, Query()] = None,
    part_number: Annotated[int | None, Query()] = None,
    content_range: Annotated[str | None, Header()] = None,
    signed: Annotated[str | None, Query()] = None,
):
    if context.check_signed_url and signed != "true":
        raise http_exception(
            "BAD_REQUEST",
            "Signed URL is required",
            400,
        )

    data = bytes(await request.body())

    if upload_type == "multipart":
        context.check_multipart_upload_session(session_token)

        etag = b64encode(hashlib.sha256(data).digest()).decode()
        context.multipart_upload_sessions[session_token][(part_number, etag)] = data

        return Response(
            status_code=206,
            media_type="text/plain",
            headers={
                "etag": etag,
                "date": _now_rfc7231(),
                "transfer-encoding": "chunked",
            },
        )
    elif upload_type == "resumable":
        context.check_resumable_upload_session(session_token)
        uploaded, completed = context.resumable_upload_sessions[session_token]

        if content_range is None:
            raise http_exception(
                "BAD_REQUEST",
                "Content-Range header is required for resumable upload",
                400,
            )
        m = _resumable_upload_content_range_pat.fullmatch(content_range)

        if not m:
            raise http_exception(
                "BAD_REQUEST",
                f"Invalid Content-Range header: {content_range}",
                400,
            )

        d = m.groupdict()
        if d["unknown_range"] != "*":
            if completed:
                raise http_exception(
                    "BAD_REQUEST",
                    "File upload has already been completed",
                    400,
                )

            start = int(d["first_pos"])
            end = int(d["last_pos"]) + 1
            total_size = None if d["total_size"] == "*" else int(d["total_size"])

            if total_size is None and (end - start) % (256 * 1024) != 0:
                raise http_exception(
                    "BAD_REQUEST",
                    f"Content-Range must be a multiple of 256KB (256 * 1024): {content_range}",
                    400,
                )

            data = uploaded + data
            completed = uploaded is not None and len(data) == total_size
            context.resumable_upload_sessions[session_token] = (data, completed)
            if completed:
                context.files[file_path] = data

        return Response(
            status_code=201 if completed else 308,
            headers={"Range": f"bytes=0-{len(data) - 1}"},
        )

    else:
        context.files[file_path] = data
        return Response(status_code=204)


@app.delete("/api/2.0/fs/files{file_path:path}")
async def delete_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    action: Annotated[str | None, Query()] = None,
    upload_type: Annotated[str | None, Query()] = None,
    session_token: Annotated[str | None, Query()] = None,
    signed: Annotated[str | None, Query()] = None,
):
    if context.check_signed_url and signed != "true":
        raise http_exception(
            "BAD_REQUEST",
            "Signed URL is required",
            400,
        )

    if action == "abort-upload":
        context.check_multipart_upload_session(session_token)
        context.multipart_upload_sessions.pop(session_token)
        return Response(status_code=204)
    if upload_type == "resumable":
        context.check_resumable_upload_session(session_token)
        context.resumable_upload_sessions.pop(session_token)
        return Response(status_code=204)
    elif action is None and upload_type is None and session_token is None:
        if file_path not in context.files:
            raise http_exception(
                "NOT_FOUND",
                "File not found",
                404,
            )

        del context.files[file_path]
        return Response(status_code=204)
    else:
        raise http_exception(
            "BAD_REQUEST",
            f"Invalid action: action={action}, upload_type={upload_type}",
            400,
        )


@app.post("/api/2.0/fs/create-download-url")
async def create_download_url(
    request: Request,
    path: Annotated[str | None, Query()] = None,
    expire_time: Annotated[datetime | None, Query()] = None,
):
    if path is None or expire_time is None:
        raise http_exception(
            "BAD_REQUEST",
            f"Missing required queries: (path, expire_time)=({path}, {expire_time})",
            400,
        )

    query = urllib.parse.urlencode({"signed": "true"})
    base = str(request.base_url).rstrip("/")

    return {
        "url": f"{base}/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
        "headers": [],
    }


@app.post("/api/2.0/fs/create-upload-part-urls")
async def create_upload_part_url(request: Request):
    body = await request.json()
    try:
        path = body["path"]
        session_token = body["session_token"]
        start_part_number = body["start_part_number"]
        count = body["count"]
        _ = body["expire_time"]
    except KeyError as e:
        raise http_exception(
            "BAD_REQUEST",
            str(e),
            400,
        ) from e

    base = str(request.base_url).rstrip("/")

    return {
        "upload_part_urls": [
            {
                "url": f"{base}/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
                "headers": [],
            }
            for query in [
                urllib.parse.urlencode(
                    {
                        "session_token": session_token,
                        "upload_type": "multipart",
                        "part_number": i,
                        "signed": "true",
                    }
                )
                for i in range(start_part_number, start_part_number + count)
            ]
        ]
    }


@app.post("/api/2.0/fs/create-resumable-upload-url")
async def create_resumable_upload_url(request: Request):
    body = await request.json()
    try:
        path = body["path"]
        session_token = body["session_token"]
    except KeyError as e:
        raise http_exception(
            "BAD_REQUEST",
            str(e),
            400,
        ) from e

    query = urllib.parse.urlencode(
        {"session_token": session_token, "upload_type": "resumable", "signed": "true"}
    )
    base = str(request.base_url).rstrip("/")

    return {
        "resumable_upload_url": {
            "url": f"{base}/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
            "headers": [],
        }
    }


@app.post("/api/2.0/fs/create-abort-upload-url")
async def create_abort_upload_url(
    request: Request,
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
):
    if not context.abort_supported:
        # As Azure Databricks, which does not support abort of multipart upload
        raise http_exception(
            "BAD_REQUEST",
            "Abort of multipart upload is not supported",
            400,
        )

    body = await request.json()
    try:
        path = body["path"]
        session_token = body["session_token"]
        _ = body["expire_time"]
    except KeyError as e:
        raise http_exception(
            "BAD_REQUEST",
            str(e),
            400,
        ) from e

    query = urllib.parse.urlencode(
        {"action": "abort-upload", "session_token": session_token, "signed": "true"}
    )
    base = str(request.base_url).rstrip("/")

    return {
        "abort_upload_url": {
            "url": f"{base}/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
            "headers": [],
        }
    }


### The Workspace API and the legacy DBFS API.
#
# They answer as their REST references say, with the SDK's error format: a JSON body of `error_code` and
# `message`, from which the SDK picks its exception class. The Workspace files and the DBFS root are separate
# namespaces, also apart from the Files API's `files`, as in Databricks.


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


@dataclass
class DummyStorage:
    """The Workspace files and the DBFS root of a dummy server."""

    workspace: dict[str, WorkspaceObject] = field(default_factory=dict)
    dbfs: dict[str, DbfsEntry] = field(default_factory=dict)
    dbfs_handles: dict[int, str] = field(default_factory=dict)
    legacy_dbfs_disabled: bool = False
    next_id: int = 1

    def __post_init__(self):
        self.reset()

    def reset(self):
        self.workspace = {"/": WorkspaceObject("DIRECTORY", 0)}
        self.dbfs = {"/": DbfsEntry(True, 0)}
        self.dbfs_handles = {}
        self.legacy_dbfs_disabled = False

    def new_id(self) -> int:
        self.next_id += 1
        return self.next_id

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


# Notebook extensions by language, which an AUTO import strips, with the header of each language's source.
_notebook_extensions = {
    ".py": ("PYTHON", b"# Databricks notebook source"),
    ".sql": ("SQL", b"-- Databricks notebook source"),
    ".scala": ("SCALA", b"// Databricks notebook source"),
    ".r": ("R", b"# Databricks notebook source"),
}
_workspace_max_size = 10 * 1024 * 1024


@app.get("/api/2.0/workspace/get-status")
async def workspace_get_status(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], path: str | None = None
):
    path = _absolute(path)
    return storage.ws_get(path).info(path)


@app.get("/api/2.0/workspace/list")
async def workspace_list(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], path: str | None = None
):
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
async def workspace_mkdirs(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
    storage.ws_mkdirs(_absolute((await _body(request)).get("path")))
    return {}


@app.post("/api/2.0/workspace/delete")
async def workspace_delete(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
    body = await _body(request)
    path = _absolute(body.get("path"))
    storage.ws_get(path)
    if _children(storage.workspace, path) and not _bool(body.get("recursive")):
        raise ApiError("DIRECTORY_NOT_EMPTY", f"Folder ({path}) is not empty.")
    for p in _subtree(storage.workspace, path):
        del storage.workspace[p]
    return {}


@app.post("/api/2.0/workspace/import")
async def workspace_import(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
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
    storage: Annotated[DummyStorage, Depends(dummy_storage)],
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
async def get_setting(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], name: str
):
    if name != "disable_legacy_dbfs":
        raise ApiError("RESOURCE_DOES_NOT_EXIST", f"Setting {name} not found.", 404)
    value = {"value": storage.legacy_dbfs_disabled}
    return {"name": name, "boolean_val": value, "effective_boolean_val": value}


### Legacy DBFS
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
async def dbfs_get_status(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], path: str | None = None
):
    _check_dbfs_enabled(storage)
    path = _dbfs_path(path)
    return storage.dbfs_get(path).info(path)


@app.get("/api/2.0/dbfs/list")
async def dbfs_list(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], path: str | None = None
):
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
async def dbfs_mkdirs(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
    _check_dbfs_enabled(storage)
    storage.dbfs_mkdirs(_dbfs_path((await _body(request)).get("path")))
    return {}


@app.post("/api/2.0/dbfs/delete")
async def dbfs_delete(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
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
async def dbfs_move(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
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
async def dbfs_create(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
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
async def dbfs_add_block(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
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
async def dbfs_close(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
    _check_dbfs_enabled(storage)
    body = await _body(request)
    _dbfs_handle(storage, body.get("handle"))
    del storage.dbfs_handles[body["handle"]]
    return {}


@app.post("/api/2.0/dbfs/put")
async def dbfs_put(
    storage: Annotated[DummyStorage, Depends(dummy_storage)], request: Request
):
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
    storage: Annotated[DummyStorage, Depends(dummy_storage)],
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
    "dummy_context",
    "dummy_storage",
    "serve_app",
]

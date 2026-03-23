"""
FastAPI application that simulates the behavior of the Databricks Files System API for testing purposes.
"""

import asyncio
import hashlib
import logging
import re
import socket
import time
import urllib.parse
import uuid
from base64 import b64encode
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import sleep
from typing import Annotated, Literal

import requests
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.params import Depends, Header
from hypercorn.asyncio import serve
from hypercorn.config import Config
from pydantic import BaseModel

log = logging.getLogger(__name__)


@dataclass
class DummyAPIContext:
    upload_mode: Literal["multipart", "resumable"] = "multipart"
    check_signed_url: bool = False

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
    return datetime.now(tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


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
            session_token = str(uuid.uuid4())
            if context.upload_mode == "multipart":
                context.multipart_upload_sessions[session_token] = {}
                return {"multipart_upload": {"session_token": session_token}}
            else:
                context.resumable_upload_sessions[session_token] = (b"", False)
                return {"resumable_upload": {"session_token": session_token}}

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

    return {
        "url": f"/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
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

    return {
        "upload_part_urls": [
            {
                "url": f"/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
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

    return {
        "resumable_upload_url": {
            "url": f"/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
            "headers": [],
        }
    }


@app.post("/api/2.0/fs/create-abort-upload-url")
async def create_abort_upload_url(request: Request):
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

    return {
        "abort_upload_url": {
            "url": f"/api/2.0/fs/files{urllib.parse.quote(path)}?{query}",
            "headers": [],
        }
    }


def _find_free_port(start_port=8000, end_port=9000) -> str:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        for port in range(start_port, end_port):
            try:
                s.bind(("127.0.0.1", port))
                return f"127.0.0.1:{port}"
            except OSError:  # noqa: PERF203
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
    "dummy_context",
    "serve_app",
]

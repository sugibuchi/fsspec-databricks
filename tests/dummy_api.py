"""
FastAPI application that simulates the behavior of the Databricks Files System API for testing purposes.
"""

import asyncio
import hashlib
import re
import socket
import time
import uuid
from base64 import b64encode
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import sleep
from typing import Annotated

import requests
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.params import Depends, Header
from hypercorn.asyncio import serve
from hypercorn.config import Config
from pydantic import BaseModel


@dataclass
class DummyAPIContext:
    files: dict[str, bytes] = field(default_factory=dict)
    upload_sessions: dict[str, dict[tuple[int, str], bytes | None]] = field(
        default_factory=dict
    )


class UploadPartUrlRequest(BaseModel):
    path: str
    session_token: str
    start_part_number: int | None = None
    count: int | None = None
    expire_time: datetime


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


_content_range_pat = re.compile(
    r"bytes (?P<first_pos>\d+)-(?P<last_pos>\d+)/(?P<length>\d+|\*)"
)


@app.get("/_headers")
async def get_headers(request: Request):
    return dict(request.headers)


@app.get("/api/2.0/fs/files{file_path:path}")
async def get_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    content_range: Annotated[str | None, Header()] = None,
):
    if file_path not in context.files:
        raise HTTPException(
            detail={
                "error_code": "NOT_FOUND",
                "message": "File not found",
            },
            status_code=404,
        )

    data = context.files[file_path]
    if content_range:
        m = _content_range_pat.fullmatch(content_range)
        if not m:
            raise HTTPException(
                detail={
                    "error_code": "BAD_REQUEST",
                    "message": "Invalid Content-Range header",
                },
                status_code=400,
            )

        first_pos = int(m.group("first_pos"))
        last_pos = int(m.group("last_pos"))
        length = m.group("length")
        if length != "*" and int(length) != last_pos - first_pos + 1:
            raise HTTPException(
                detail={
                    "error_code": "BAD_REQUEST",
                    "message": "Content-Range length does not match the specified range",
                },
                status_code=400,
            )
        return Response(
            status_code=206,
            media_type="application/octet-stream",
            headers={
                "content-range": f"bytes {first_pos}-{last_pos}/{last_pos - first_pos + 1}",
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
    part_number: Annotated[int | None, Query()] = None,
):
    if not overwrite and file_path in context.files:
        raise HTTPException(
            detail={
                "error_code": "RESOURCE_ALREADY_EXISTS",
                "message": f"File already exists: {file_path}",
            },
            status_code=409,
        )

    if action is not None:
        if action == "initiate-upload":
            session_token = str(uuid.uuid4())
            context.upload_sessions[session_token] = {}
            return {"multipart_upload": {"session_token": session_token}}

        if upload_type != "multipart":
            raise HTTPException(
                detail={
                    "error_code": "BAD_REQUEST",
                    "message": f"Invalid upload_type: {upload_type}",
                },
                status_code=400,
            )

        if action == "upload-part":
            data = await request.body()
            etag = b64encode(hashlib.sha256(data).digest()).decode()
            context.upload_sessions[session_token][(part_number, etag)] = data

            return Response(
                status_code=206,
                media_type="text/plain",
                headers={
                    "etag": etag,
                    "date": _now_rfc7231(),
                    "transfer-encoding": "chunked",
                },
            )

        elif action == "complete-upload":
            parts = [
                (p.part_number, p.etag)
                for p in Parts.model_validate(await request.json()).parts
            ]

            parts.sort()

            session = context.upload_sessions.pop(session_token)

            data = bytearray()
            for p in sorted(parts):
                if p not in session:
                    raise HTTPException(
                        detail={
                            "error_code": "BAD_REQUEST",
                            "message": f"Invalid part number or etag: {p}",
                        },
                        status_code=400,
                    )

                if session[p] is None:
                    raise HTTPException(
                        detail={
                            "error_code": "BAD_REQUEST",
                            "message": f"Data for part number {p[0]} is not uploaded",
                        },
                        status_code=400,
                    )
                data.extend(session[p])
            context.files[file_path] = bytes(data)

            return Response(status_code=206)
        else:
            raise HTTPException(
                detail={
                    "error_code": "BAD_REQUEST",
                    "message": f"Invalid action: {action}",
                },
                status_code=400,
            )
    else:
        context.files[file_path] = bytes(await request.body())
        return Response(status_code=206)


@app.put("/api/2.0/fs/files{file_path:path}")
async def put_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    request: Request,
):
    context.files[file_path] = bytes(await request.body())
    return Response(status_code=204)


@app.delete("/api/2.0/fs/files{file_path:path}")
async def delete_file(
    context: Annotated[DummyAPIContext, Depends(dummy_context)],
    file_path: str,
    action: Annotated[str | None, Query()] = None,
    session_token: Annotated[str | None, Query()] = None,
    upload_type: Annotated[str | None, Query()] = None,
):
    if action:
        if action == "abort-upload":
            if upload_type == "multipart" and session_token in context.upload_sessions:
                context.upload_sessions.pop(session_token)
                return Response(status_code=204)
            else:
                raise HTTPException(
                    detail={
                        "error_code": "BAD_REQUEST",
                        "message": f"Invalid upload_type or session_token: {upload_type}, {session_token}",
                    },
                    status_code=400,
                )
        else:
            raise HTTPException(
                detail={
                    "error_code": "BAD_REQUEST",
                    "message": f"Invalid action: {action}",
                },
                status_code=400,
            )
    else:
        if file_path not in context.files:
            raise HTTPException(
                detail={"error_code": "NOT_FOUND", "message": "File not found"},
                status_code=404,
            )

        del context.files[file_path]
        return Response(status_code=204)


@app.post("/api/2.0/fs/create-upload-part-urls")
async def create_upload_part_url(
    request: UploadPartUrlRequest,
    host: Annotated[str, Header()],
):
    return {
        "upload_part_urls": [
            {
                "part_number": i,
                "url": f"http://{host}/api/2.0/fs/files{request.path}?action=upload-part&upload_type=multipart&session_token={request.session_token}&part_number={i}",
            }
            for i in range(
                request.start_part_number, request.start_part_number + request.count
            )
        ]
    }


@app.post("/api/2.0/fs/create-abort-upload-url")
async def create_abort_upload_url(
    request: UploadPartUrlRequest,
    host: Annotated[str, Header()],
):
    return {
        "abort_upload_url": {
            "url": f"http://{host}/api/2.0/fs/files{request.path}?action=abort-upload&upload_type=multipart&session_token={request.session_token}",
        },
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
        loop.call_soon_threadsafe(shutdown_event.set)
        future.result()


__all__ = [
    "dummy_context",
    "serve_app",
]

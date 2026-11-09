"""Tests of the HTTP helpers shared by the Volume and DBFS files. The files' requests are tested with their backends."""

import errno

import pytest
from aiohttp import ClientConnectionError, ClientResponseError, RequestInfo
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from fsspec_databricks.http_client import (
    _error_code,
    _retry_after,
    _transient_cause,
    response_error,
)


def _status_error(status: int) -> ClientResponseError:
    info = RequestInfo(URL("http://localhost"), "GET", CIMultiDictProxy(CIMultiDict()))
    return ClientResponseError(info, (), status=status)


@pytest.mark.parametrize(
    ("status", "error_code", "expected", "error_no"),
    [
        (404, None, FileNotFoundError, errno.ENOENT),
        (400, "RESOURCE_DOES_NOT_EXIST", FileNotFoundError, errno.ENOENT),
        (401, None, PermissionError, errno.EACCES),
        (403, "PERMISSION_DENIED", PermissionError, errno.EACCES),
        (409, None, FileExistsError, errno.EEXIST),
        (400, "RESOURCE_ALREADY_EXISTS", FileExistsError, errno.EEXIST),
        (400, "MAX_BLOCK_SIZE_EXCEEDED", OSError, errno.EIO),
        (500, None, OSError, errno.EIO),
    ],
)
def test_response_error(status, error_code, expected, error_no):
    e = response_error("/a/b", _status_error(status), "Doing", error_code)
    assert type(e) is expected
    assert (e.errno, e.filename) == (error_no, "/a/b")
    assert f"Doing failed with status {status}" in e.strerror
    if error_code:
        assert error_code in e.strerror


def test_response_error_without_status():
    e = response_error("/a/b", ClientConnectionError(), "Doing")
    assert (type(e), e.errno) == (OSError, errno.EIO)


def _response_oserror(status: int) -> OSError:
    cause = _status_error(status)
    e = response_error("/a/b", cause, "Doing")
    e.__cause__ = cause
    return e


@pytest.mark.parametrize(
    ("error", "transient"),
    [
        (_status_error(503), True),
        (_status_error(429), True),
        (_status_error(404), False),
        (_response_oserror(500), True),
        (_response_oserror(403), False),
        (ClientConnectionError(), True),
        (TimeoutError(), True),
        (OSError(errno.EIO, "Changed"), False),
        (ValueError(), False),
    ],
)
def test_transient_cause(error, transient):
    assert (_transient_cause(error) is not None) is transient


def test_retry_after():
    info = RequestInfo(URL("http://localhost"), "GET", CIMultiDictProxy(CIMultiDict()))
    headers = CIMultiDictProxy(CIMultiDict({"Retry-After": "7"}))
    assert _retry_after(ClientResponseError(info, (), status=429, headers=headers)) == 7
    # An HTTP date is ignored: the delay then doubles.
    headers = CIMultiDictProxy(
        CIMultiDict({"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    )
    assert (
        _retry_after(ClientResponseError(info, (), status=429, headers=headers)) is None
    )
    assert _retry_after(_status_error(503)) is None


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            '{"error_code": "RESOURCE_ALREADY_EXISTS", "message": "x"}',
            "RESOURCE_ALREADY_EXISTS",
        ),
        ('{"message": "x"}', None),
        ("[1]", None),
        ("<html>Bad gateway</html>", None),
        ("", None),
        (None, None),
    ],
)
def test_error_code(body, expected):
    assert _error_code(body) == expected

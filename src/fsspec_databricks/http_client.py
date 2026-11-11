import asyncio
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from random import random
from typing import TypeVar

from aiohttp import (
    ClientConnectionError,
    ClientError,
    ClientHandlerType,
    ClientMiddlewareType,
    ClientRequest,
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    TCPConnector,
)
from databricks.sdk.client_types import HostType
from databricks.sdk.config import Config
from fsspec.asyn import sync

from .error import access_error, file_exists_error, file_not_found_error, io_error

T = TypeVar("T")

logger = logging.getLogger(__name__)


class AioHttpSessionSupport:
    """The aiohttp session of a file system whose files call the REST APIs directly. A mixin.

    The file system must have `closed`, as `DatabricksClientSupport` does.
    """

    connection_pool_size: int = 100
    """The maximum number of connections in the aiohttp connection pool (TCPConnector limit)."""

    def _init_session(self, connection_pool_size: int | None = None) -> None:
        if connection_pool_size is not None:
            self.connection_pool_size = connection_pool_size
        # Made on first use, as TCPConnector needs a running loop. A file system runs on one loop, as fsspec caches
        # it: the session's loop is kept only to close it there.
        self._session: ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None

    def close(self):
        """Close the file system and its HTTP session."""
        try:
            self._close_session()
        finally:
            super().close()

    def _close_session(self) -> None:
        session, self._session = self._session, None
        loop, self._session_loop = self._session_loop, None
        if session is None or session.closed or loop.is_closed():
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            # Nothing can wait for it on its own loop.
            loop.create_task(session.close())
        elif loop.is_running() and not sys.is_finalizing():
            sync(loop, session.close, timeout=5.0)

    async def _get_session(self) -> ClientSession:
        """The aiohttp session of the file system, made on first use."""
        if self.closed:
            raise RuntimeError("The file system is already closed")
        if self._session is None:
            self._session = ClientSession(
                connector=TCPConnector(limit=self.connection_pool_size)
            )
            self._session_loop = asyncio.get_running_loop()
        return self._session


def workspace_authenticator(config: Config) -> ClientMiddlewareType:
    """An aiohttp middleware adding the SDK `Config`'s authentication headers to each request."""
    cfg = config

    async def auth(
        request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        ### From https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/core.py
        headers = await asyncio.to_thread(cfg.authenticate)
        # Add X-Databricks-Org-Id header for workspace clients on unified hosts
        if cfg.workspace_id and cfg.host_type == HostType.UNIFIED:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id
        ### end

        request.headers.update(headers)
        return await handler(request)

    return auth


retryable_statuses = frozenset({429, 500, 502, 503, 504})


def response_error(
    path: str, error: Exception, doing: str, error_code: str | None = None
) -> OSError:
    """An OSError for a failed request, by its HTTP status or the API's error code."""
    status = getattr(error, "status", None)
    message = f"{doing} failed" + (f" with status {status}" if status else "")
    if error_code:
        message += f", {error_code}"
    if status == 404 or error_code == "RESOURCE_DOES_NOT_EXIST":
        return file_not_found_error(path, message)
    if status in (401, 403):
        return access_error(path, message)
    # DBFS answers an existing file with a 400.
    if status == 409 or error_code == "RESOURCE_ALREADY_EXISTS":
        return file_exists_error(path, message)
    else:
        return io_error(path, message)


def _error_code(body: str | None) -> str | None:
    """The `error_code` of a Databricks API error's JSON body, if it has one."""
    try:
        error = json.loads(body or "")
    except ValueError:
        return None
    return error.get("error_code") if isinstance(error, dict) else None


async def check_response(response: ClientResponse, path: str, doing: str) -> None:
    """Log the request, and raise an OSError for a failed response, logging its body."""
    if response.ok:
        logger.debug("%s: path=%s, status=%d", doing, path, response.status)
        return
    try:
        body = await response.text()
    except ClientError:
        # The server may close the connection before a refused upload is sent.
        body = None
    logger.debug(
        "%s: path=%s, status=%d, response=%s", doing, path, response.status, body
    )
    try:
        response.raise_for_status()
    except ClientResponseError as e:
        raise response_error(path, e, doing, _error_code(body)) from e


class HttpFileMixin:
    """Request settings shared by the file classes calling the REST APIs with aiohttp.

    The timeouts are those of aiohttp's `ClientTimeout`, in seconds, and `None` or `0` means none. See the `aiohttp
    documentation <https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts>`_.
    """

    timeout_total: float | None = None
    """The maximum seconds for a whole request: connecting, sending and reading the response."""

    timeout_connect: float | None = None
    """The maximum seconds to get a connection, including the wait for a free one from the pool."""

    timeout_sock_connect: float | None = 30
    """The maximum seconds to connect to a peer for a new connection."""

    timeout_sock_read: float | None = 120
    """The maximum seconds between two reads of data from a peer."""

    max_retries: int = 3
    """The maximum number of retries of a request that is safe to send again, after a transient error."""

    retry_delay: float = 1.0
    """The base delay in seconds before the first retry. It doubles with each retry, unless the answer's
    `Retry-After` sets it."""

    def _init_http(
        self, session: ClientSession, base_url: str, auth: ClientMiddlewareType
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._auth = auth
        self._timeout = ClientTimeout(
            total=self.timeout_total,
            connect=self.timeout_connect,
            sock_connect=self.timeout_sock_connect,
            sock_read=self.timeout_sock_read,
        )

    async def _retried(self, send: Callable[[], Awaitable[T]], doing: str) -> T:
        """Send a request, and again after a transient error, as the SDK does: a 429 or 5xx answer, or a lost connection.

        Only for requests that are safe to send again.
        """
        delay = self.retry_delay
        for _ in range(self.max_retries):
            try:
                return await send()
            except (OSError, ClientError) as e:
                cause = _transient_cause(e)
                if cause is None:
                    raise
                logger.warning(
                    "Retrying after %s: %s, path=%s",
                    getattr(cause, "status", None) or type(cause).__name__,
                    doing,
                    self.path,
                )
                retry_after = _retry_after(cause)
            # The jitter keeps concurrent requests from retrying together.
            await asyncio.sleep(
                retry_after if retry_after is not None else delay * (1 + random())
            )
            delay *= 2
        return await send()


def _transient_cause(error: BaseException) -> BaseException | None:
    """The transient failure behind an error, if a retry may succeed: also behind the `OSError` of a response."""
    if isinstance(error, OSError) and isinstance(error.__cause__, ClientError):
        error = error.__cause__
    if isinstance(error, ClientResponseError):
        return error if error.status in retryable_statuses else None
    else:
        return (
            error if isinstance(error, ClientConnectionError | TimeoutError) else None
        )


def _retry_after(error: BaseException) -> float | None:
    """The seconds of an answer's `Retry-After`, if it has one in seconds."""
    headers = getattr(error, "headers", None)
    try:
        return float(headers["Retry-After"]) if headers else None
    except (KeyError, TypeError, ValueError):
        return None


class HttpReadableMixin(HttpFileMixin):
    """Range reads retried after transient errors. The file implements `_request_range`."""

    async def _request_range(self, start: int, end: int) -> bytes:
        """Fetch the range, raising aiohttp's errors."""
        raise NotImplementedError

    async def _fetch_range(self, start: int, end: int) -> bytes:
        doing = f"Reading the range {start}-{end}"
        try:
            return await self._retried(lambda: self._request_range(start, end), doing)
        except (ClientError, TimeoutError) as e:
            raise response_error(self.path, e, doing) from e


__all__ = [
    "AioHttpSessionSupport",
    "HttpFileMixin",
    "HttpReadableMixin",
    "check_response",
    "response_error",
    "retryable_statuses",
    "workspace_authenticator",
]

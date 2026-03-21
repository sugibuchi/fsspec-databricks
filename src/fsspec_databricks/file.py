import asyncio
import errno
import logging
import math
import os
import re
from abc import ABC, abstractmethod
from asyncio import CancelledError
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import contextmanager
from io import BytesIO, RawIOBase, UnsupportedOperation
from tempfile import NamedTemporaryFile
from time import perf_counter
from typing import Any

from .error import file_exists_error, file_not_found_error, io_error, os_error


class AbstractFile(RawIOBase):
    """Base class of file-like objects."""

    log: logging.Logger = logging.getLogger(__name__)

    verbose_debug_log: bool = False
    """Whether to enable verbose debug logging for file operations."""

    def __init__(
        self,
        path: str,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__()
        self.path = path

        if verbose_debug_log is not None:
            self.verbose_debug_log = verbose_debug_log

    def _ensure_not_closed(self) -> bool:
        """Raise an exception if the file is not ready for reading/writing. Otherwise, return True.

        Returns
        -------
        bool
            Always returns True.
        """
        if self.closed:
            raise ValueError(f"I/O operation on closed file: path={self.path}")

        return True

    def close(self):
        if not self.closed:
            super().close()
            self.log.debug("Closed: path=%s", self.path)

    def __repr__(self):
        return f"{self.__class__.__name__}(path={self.path})"


class AbstractAsyncFile(AbstractFile):
    """Abstract file-like object for accessing files by using a background `asyncio` event loop."""

    def __init__(
        self,
        path: str,
        loop,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(path=path, verbose_debug_log=verbose_debug_log)

        self._loop = loop

    def _run_and_wait(self, async_func: Callable[..., Coroutine], *args, **kwargs):
        """Run an asynchronous function in the background event loop and wait for the result."""
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop == self._loop:
            raise RuntimeError(
                "Run a synchronous operation in the same event loop attached to the file object is not allowed. "
                f"Use the asynchronous API with await instead.: path={self.path}"
            )

        if not self._loop.is_running():
            raise RuntimeError(f"Event loop is not running: path={self.path}")

        if self.verbose_debug_log:
            self.log.debug("Run %s synchronously: path=%s", async_func, self.path)

        return asyncio.run_coroutine_threadsafe(
            async_func(*args, **kwargs), self._loop
        ).result()

    async def aclose(self):
        """Asynchronously flush and close the file, releasing any resources associated with it."""
        if not self.closed:
            try:
                super().close()
            finally:
                self._loop = None

    def close(self):
        if not self.closed:
            self._run_and_wait(self.aclose)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()


class _FileRangeTask(Awaitable):
    def __init__(
        self,
        start: int,
        end: int,
        task: asyncio.Task,
    ):
        self.start = start
        self.end = end
        self.task = task

    def get_name(self) -> str:
        return self.task.get_name()

    def done(self):
        return self.task.done()

    def cancel(self, msg: Any | None = None) -> bool:
        return self.task.cancel(msg)

    def __await__(self):
        return self.task.__await__()

    def __repr__(self):
        task_repr = " ".join(repr(self.task).split(" ", 3)[0:3]) + ">"
        return f"_FileRangeTask(start={self.start}, end={self.end}, task={task_repr})"


class FileRangeTaskSupport(AbstractAsyncFile):
    """Abstract file-like object that supports asynchronous concurrent access to byte ranges of a file
    by using a background asyncio event loop."""

    default_max_concurrency: int = 10
    """The maximum number of concurrent tasks for accessing the file."""

    def __init__(
        self,
        path: str,
        loop,
        max_concurrency: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            loop=loop,
            verbose_debug_log=verbose_debug_log,
        )

        self._semaphore: asyncio.Semaphore
        self.max_concurrency = max_concurrency or self.default_max_concurrency

        self._tasks: deque[_FileRangeTask] = deque()

        self._task_name_prefix = "file-" + re.sub(
            r"\W+", "-", self.path.rsplit("/", 1)[-1].lower()
        )

        self._task_error: BaseException | None = None

    @property
    def max_concurrency(self) -> int:
        """The maximum number of concurrent tasks for accessing byte ranges of the file."""
        return self._max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value: int):
        if value <= 0:
            raise ValueError("max_concurrency must be a positive integer")

        self._max_concurrency = value
        self._semaphore = asyncio.Semaphore(value)

    async def _schedule_task(
        self,
        start: int,
        end: int,
        async_func: Callable[..., Coroutine],
        *args,
        **kwargs,
    ):
        """Schedule execution the async function as a task for the byte range [start, end).

        This method waits until an available slot becomes available to run the task (based on the `max_concurrency`).
        """

        if self._task_error:
            raise io_error(
                self.path,
                f"Schedule of {async_func} is blocked due to a previous error during file access: path={self.path}",
            ) from self._task_error

        def on_task_done(t: asyncio.Task):
            self._semaphore.release()

            if t.cancelled():
                if self.verbose_debug_log:
                    self.log.debug(
                        "Task %s was cancelled: path=%s",
                        t.get_name(),
                        self.path,
                    )
            elif t.exception():
                self._task_error = t.exception()
                self.log.error(
                    "Task %s raised exception: path=%s",
                    t.get_name(),
                    self.path,
                    exc_info=t.exception(),
                )
            elif self.verbose_debug_log:
                self.log.debug(
                    "Task %s done in %.2f sec: path=%s",
                    t.get_name(),
                    perf_counter() - begin,
                    self.path,
                )

        await self._semaphore.acquire()
        try:
            begin = perf_counter()
            coro = async_func(*args, **kwargs)
            task = asyncio.create_task(
                coro, name=f"{self._task_name_prefix}-{start}-{end}"
            )
            task.add_done_callback(on_task_done)
        except:
            self._semaphore.release()
            raise

        self._tasks.append(_FileRangeTask(start=start, end=end, task=task))

        if self.verbose_debug_log:
            self.log.debug("Task %s scheduled: path=%s", task.get_name(), self.path)

    async def _cancel_task(
        self,
        task: _FileRangeTask,
    ) -> tuple[bool, BaseException | None]:
        """Cancel the given task and wait for it to be canceled.

        Returns
        -------
        tuple[bool, BaseException | None]
            Boolean whether the task was canceled, and the exception raised during the cancellation if any.
        """
        if not task.done() and self.verbose_debug_log:
            self.log.debug("Cancelling task: %s", task.get_name())

        canceled = task.cancel()
        exception = None

        try:
            await task
        except CancelledError:
            pass
        except BaseException as e:
            exception = e

        if canceled and self.verbose_debug_log:
            self.log.debug("Cancelled task: %s", task.get_name())

        return canceled, exception

    async def _cancel_all_tasks(self):
        """Cancel all ongoing tasks."""
        to_cancel = sum(not t.task.done() for t in self._tasks)

        if to_cancel and self.verbose_debug_log:
            self.log.debug(
                "Attempting to cancel %s tasks: path=%s", to_cancel, self.path
            )

        tasks = list(self._tasks)
        self._tasks.clear()

        results = await asyncio.gather(*[self._cancel_task(t) for t in tasks])

        num_canceled = sum(1 for canceled, _ in results if canceled)
        num_exceptions = sum(1 for _, exception in results if exception)

        if num_canceled and self.verbose_debug_log:
            self.log.debug("Cancelled %s tasks: path=%s", num_canceled, self.path)
        if num_exceptions:
            self.log.error(
                "%s tasks raised exceptions during task cancellation: path=%s",
                num_exceptions,
                self.path,
            )

    async def aclose(self):
        if not self.closed:
            try:
                await self._cancel_all_tasks()
            finally:
                await super().aclose()


class AbstractAsyncReadableFile(FileRangeTaskSupport, ABC):
    """Abstract file-like object that supports asynchronous reading of a file by using a background asyncio event loop."""

    min_block_size: int = 512 * 1024
    """The minimum data size to read for each read operation on the file."""

    max_block_size: int = 4 * 1024 * 1024
    """The maximum data size to read for each read operation on the file."""

    def __init__(
        self,
        path: str,
        loop,
        size: int,
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            loop=loop,
            max_concurrency=max_concurrency,
            verbose_debug_log=verbose_debug_log,
        )

        self._file_size = size
        self._pos = 0

        self._read_length = 0
        """The total length of data that has been read sequentially from the file.
        This is used to determine the block size for the next fetch task.
        This variable is reset when a seek operation is performed."""

        self.min_block_size = block_size or min_block_size or self.min_block_size
        self.max_block_size = block_size or max_block_size or self.max_block_size

        self._task_name_prefix = f"fetch-{self._task_name_prefix}"

    @abstractmethod
    async def _fetch_range(self, start: int, end: int) -> bytes:
        """Fetch the byte range [start, end) of the file.

        Parameters
        ----------
        start : int
            The starting byte offset (inclusive) of the range to fetch.
        end : int
            The ending byte offset (exclusive) of the range to fetch.

        Returns
        -------
        bytes
            The bytes in the specified range.
        """

    async def _schedule_fetch_task(self, start: int, end: int):
        await self._schedule_task(start, end, self._fetch_range, start, end)

    async def _read(self, size: int = -1) -> bytes:  # noqa: C901
        self._ensure_not_closed()

        if self.verbose_debug_log:
            self.log.debug(
                "Read requested: path=%s, size=%s, pos=%s",
                self.path,
                size,
                self._pos,
            )

        if size == 0 or self._pos >= self._file_size:
            if self.verbose_debug_log:
                self.log.debug(
                    "Returning data: path=%s, size=0, pos=%s",
                    self.path,
                    self._pos,
                )
            return b""

        # Start fetching data from the end of ongoing tasks or the current position if there is no ongoing task.
        fetch_start = self._tasks[-1].end if self._tasks else self._pos

        if size < 0:
            # Optimize the block size to fetch the all remaining content of the file.
            self._read_length += self._file_size - self._pos

            fetch_end = self._file_size
            fetch_length = fetch_end - fetch_start

            block_size = max(
                min(
                    self.max_block_size, math.ceil(fetch_length / self.max_concurrency)
                ),
                self.min_block_size,
            )
        else:
            # Update the consecutive read length
            self._read_length += size

            # Determine the data block size based on _read_length
            block_size = self.min_block_size
            while (
                # If n-bytes have been consecutively read, we assume another n-bytes to be read consecutively.
                # We adjust the block size to make (block size x max concurrency) cover this n-bytes.
                block_size * self.max_concurrency < self._read_length
                # But we take the remaining bytes in the file into account.
                and block_size * self.max_concurrency < self._file_size - fetch_start
                # And we also limit the max block size.
                and block_size < self.max_block_size
            ):
                block_size *= 2
                block_size = min(block_size, self.max_block_size)

            fetch_end = min(
                max(
                    # At least, fetch the current pos + size
                    self._pos + size,
                    # If n-bytes have been consecutively read, fetch another n-bytes speculatively
                    self._pos + self._read_length,
                ),
                # Max prefetch size
                self._pos + size + block_size * self.max_concurrency,
                # Don't try to fetch beyond the end of the file
                self._file_size,
            )

        # Number of the data block to fetch
        block_count = math.ceil((fetch_end - fetch_start) / block_size)

        if block_count > 0:
            # Adjust fetch_end to align with the block size
            fetch_end = min(
                fetch_start + block_count * block_size,
                self._file_size,
            )

            if self.verbose_debug_log:
                self.log.debug(
                    "Scheduling data fetch: path=%s, start=%s, end=%s, block_count=%s",
                    self.path,
                    fetch_start,
                    fetch_end,
                    block_count,
                )

            # Schedule fetch tasks for each data block
            for s in range(fetch_start, fetch_end, block_size):
                await self._schedule_fetch_task(s, min(s + block_size, fetch_end))

        data = bytearray()

        # Await and concat fetched byte chunks
        task = None
        while self._tasks:
            task = self._tasks.popleft()
            chunk = memoryview(await task)
            chunk = chunk[max(0, self._pos - task.start) :]
            if size > 0:
                data.extend(chunk[: min(len(chunk), size - len(data))])
                if size == len(data):
                    break
            else:
                data.extend(chunk)

        # Update the file position
        self._pos += len(data)

        # If the task has remaining bytes after the new file position, put it back to the front of the task list.
        if task and task.end > self._pos:
            self._tasks.appendleft(task)

        if self.verbose_debug_log:
            self.log.debug(
                "Returning data: path=%s, size=%s, pos=%s",
                self.path,
                len(data),
                self._pos,
            )

        return bytes(data)

    def readable(self):
        self._ensure_not_closed()
        return True

    def read(self, size: int = -1) -> bytes:
        self._ensure_not_closed()
        return self._run_and_wait(self._read, size)

    def tell(self) -> int:
        self._ensure_not_closed()
        return self._pos

    async def _seek(self, offset: int, whence: int = 0) -> int:
        self._ensure_not_closed()

        if self.verbose_debug_log:
            self.log.debug(
                "Seek requested: path=%s, pos=%s, offset=%s, whence=%s",
                self.path,
                self._pos,
                offset,
                whence,
            )

        if whence == 0:
            new_pos = offset
        elif whence == 1:
            new_pos = self._pos + offset
        elif whence == 2:
            new_pos = self._file_size + offset
        else:
            raise ValueError(f"Invalid whence {whence}: path={self.path}")

        if new_pos < 0 or new_pos > self._file_size:
            raise ValueError(
                f"Seek position {new_pos} out of the file bound: path={self.path}"
            )

        self._pos = new_pos

        if self.verbose_debug_log:
            self.log.debug(
                "Calculated new position: path=%s, pos=%s",
                self.path,
                self._pos,
            )

        # Cancel all tasks if the new position is outside the ranges of ongoing tasks.
        if self._tasks and not (
            self._tasks[0].start <= self._pos < self._tasks[-1].end
        ):
            await self._cancel_all_tasks()

        # Cancel tasks before the new position
        to_cancel = []
        while self._tasks and self._pos >= self._tasks[0].end:
            to_cancel.append(self._tasks.popleft())
        if to_cancel:
            await asyncio.gather(*[self._cancel_task(t) for t in to_cancel])

        # Reset the consecutive read length
        self._read_length = 0

        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        self._ensure_not_closed()
        return self._run_and_wait(self._seek, offset, whence)


class AbstractAsyncWritableFile(FileRangeTaskSupport, ABC):
    """Abstract file-like object that supports asynchronous writing to a file by using a background asyncio event loop."""

    min_block_size: int = 1024 * 1024
    """The minimum data size to write for each write operation on the file."""

    max_block_size: int = 16 * 1024 * 1024
    """The maximum data size to write for each write operation on the file."""

    min_multipart_upload_size: int = 5 * 1024 * 1024
    """The minimum file size to use multipart upload for uploading the file content."""

    def __init__(
        self,
        path: str,
        loop,
        max_concurrency: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        min_multipart_upload_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            loop=loop,
            max_concurrency=max_concurrency,
            verbose_debug_log=verbose_debug_log,
        )

        self.min_block_size = block_size or min_block_size or self.min_block_size
        self.max_block_size = block_size or max_block_size or self.max_block_size
        self.min_multipart_upload_size = (
            min_multipart_upload_size or self.min_multipart_upload_size
        )

        self._pos = 0
        self._buf = BytesIO()

        self._multipart_uploading = False
        self._part_index = 0

        self._task_name_prefix = f"upload-{self._task_name_prefix}"

    @abstractmethod
    async def _upload_all(self, data: bytes) -> None:
        """Upload the given data to the file by a single upload request."""

    @abstractmethod
    async def _start_multipart_upload(self) -> None:
        """Start upload for the file."""

    @abstractmethod
    async def _upload_part(
        self, data: bytes, start: int, end: int, part_index: int, last_part: bool
    ) -> Any:
        """Upload a part of the file and return the part token."""

    @abstractmethod
    async def _complete_multipart_upload(self) -> None:
        """Complete the upload by committing all the uploaded parts."""

    @abstractmethod
    async def _abort_multipart_upload(self) -> None:
        """Abort the upload and discard all the uploaded parts."""

    async def _do_upload_all(self, data: bytes) -> None:
        if self._multipart_uploading:
            raise RuntimeError(f"Upload has already been started: path={self.path}")
        try:
            self.log.debug(
                "Uploading %d bytes by a single upload request: path=%s",
                len(data),
                self.path,
            )
            begin = perf_counter()
            await self._upload_all(data)
            self.log.debug(
                "Uploaded %d bytes in %.2f sec: path=%s",
                len(data),
                perf_counter() - begin,
                self.path,
            )

        except Exception as e:
            raise io_error(self.path, "Failed to upload all data.") from e

    async def _do_start_multipart_upload(self) -> None:
        if self._multipart_uploading:
            raise RuntimeError(f"Upload has already been started: path={self.path}")

        try:
            self.log.debug("Starting multipart upload: path=%s", self.path)
            await self._start_multipart_upload()
            self._multipart_uploading = True
            self.log.debug("Multipart upload started: path=%s", self.path)
        except Exception as e:
            raise io_error(self.path, "Failed to start upload.") from e

    async def _do_complete_multipart_upload(self) -> None:
        if not self._multipart_uploading:
            raise RuntimeError(f"Upload has not been started: path={self.path}")

        try:
            self.log.debug("Completing multipart upload: path=%s", self.path)
            await self._complete_multipart_upload()
            self.log.debug("Multipart upload completed: path=%s", self.path)
        except Exception as e:
            await self._do_abort_multipart_upload()
            raise io_error(self.path, "Failed to complete upload.") from e
        finally:
            self._multipart_uploading = False
            await self._cancel_all_tasks()

    async def _do_abort_multipart_upload(self) -> None:
        if not self._multipart_uploading:
            raise RuntimeError(f"Upload has not been started: path={self.path}")

        try:
            self.log.warning("Aborting multipart upload: path=%s", self.path)
            await self._abort_multipart_upload()
            self.log.warning("Multipart upload aborted: path=%s", self.path)
        except Exception as e:
            raise io_error(self.path, "Failed to abort upload.") from e
        finally:
            self._multipart_uploading = False
            await self._cancel_all_tasks()

    async def _upload_buffered_data(self, flush=False):  # noqa: C901
        data = self._buf.getbuffer()
        buffered_size = len(data)

        if buffered_size == 0:
            return

        if (
            not self._multipart_uploading
            and buffered_size < self.min_multipart_upload_size
        ):
            # If the multipart upload has not started and the buffered data size is smaller than
            # the minimum multipart upload size:
            if flush:
                # If flushing the buffer, upload all buffered data in a single upload request.
                _data = data.tobytes()
                del data
                self._buf.seek(0)
                self._buf.truncate()

                await self._do_upload_all(_data)
                return
            else:
                # Otherwise, wait for more data to be buffered
                return

        # Target data size to upload. When flushing the buffer, the target size is the total buffered data size.
        # Otherwise, if n-bytes have already been written, assume another n-bytes as the target.
        target_size = buffered_size if flush else self._pos

        # Determine the data block size for upload tasks
        block_size = self.min_block_size
        while (
            # We adjust the block size make (block size x max concurrency) cover the target data upload size.
            block_size * self.max_concurrency < target_size
            # But we also limit the max block size.
            and block_size < self.max_block_size
        ):
            block_size *= 2
            block_size = min(block_size, self.max_block_size)

        offset = self._pos - buffered_size
        uploaded = 0

        while data:
            # Stop uploading if the remaining data size falls below the minimum block size...
            if len(data) - block_size < self.min_block_size:
                if flush:
                    # ... except when flushing the buffer.
                    upload_size = len(data)
                else:
                    break
            else:
                upload_size = block_size

            start = offset
            end = offset + upload_size

            if not self._multipart_uploading:
                await self._do_start_multipart_upload()

            await self._schedule_task(
                start,
                end,
                self._upload_part,
                data[:upload_size].tobytes(),
                start,
                end,
                self._part_index,
                flush and len(data) == upload_size,
            )

            offset += upload_size
            uploaded += upload_size
            data = data[upload_size:]
            self._part_index += 1

        if uploaded:
            remaining = data.tobytes()
            del data

            self._buf.seek(0)
            self._buf.truncate(0)
            self._buf.write(remaining)

            if self.verbose_debug_log:
                self.log.debug(
                    "Data upload was scheduled: path=%s, size=%s",
                    self.path,
                    uploaded,
                )

        if flush:
            self.log.debug(
                "Waiting for the completion of all upload tasks: path=%s",
                self.path,
            )
            await asyncio.gather(*self._tasks)

    def writable(self):
        self._ensure_not_closed()
        return True

    async def _write(self, data: bytes) -> int:
        self._ensure_not_closed()

        if self.verbose_debug_log:
            self.log.debug(
                "Write requested data: path=%s, size=%s, pos=%s",
                self.path,
                len(data),
                self._pos,
            )

        self._buf.write(data)
        self._pos += len(data)

        await self._upload_buffered_data()
        return len(data)

    def write(self, data: bytes) -> int:
        self._ensure_not_closed()
        return self._run_and_wait(self._write, data)

    def flush(self):
        self._ensure_not_closed()
        # No eager flush operation to avoid uploading too small data blocks.

    async def aclose(self):
        if not self.closed:
            try:
                if self._task_error:
                    if self._multipart_uploading:
                        await self._do_abort_multipart_upload()
                        raise io_error(
                            self.path, "Multipart upload failed and was aborted"
                        ) from self._task_error
                else:
                    await self._upload_buffered_data(flush=True)
                    if self._multipart_uploading:
                        await self._do_complete_multipart_upload()
            finally:
                self._buf = None
                await super().aclose()


class AbstractCachedFile(AbstractFile, ABC):
    """Abstract file-like object that supports reading/writing a remote file by caching the entire file content
    in a local resource."""

    max_size: int | None = None
    """The maximum size of the file to read or write. If None, there is no limit on the file size."""

    block_size: int = 1024 * 1024  # 1 MB
    """The size of data block to use when downloading/uploading the remote file content."""

    def __init__(
        self,
        path: str,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(path=path, verbose_debug_log=verbose_debug_log)

        self.max_size = max_size or self.max_size
        self.block_size = block_size or self.block_size

        self.mode = mode
        self._readable = read_mode(mode)
        self._writable = write_mode(mode)

        self._cache = None
        self._updated = False

        if max_size is not None and max_size <= 0:
            raise ValueError("max_size must be a positive integer or None")

        if "x" in mode and self._do_remote_file_exists():
            raise file_exists_error(path)

        self._cache = self._initialize_cache()
        try:
            if "r" in mode or "a" in mode:
                if self._do_remote_file_exists():
                    self._download_remote_file_to_cache()
                    if "r" in mode:
                        self._cache.seek(0)
                elif "r" in mode:
                    raise file_not_found_error(path)
        except:
            # Eagerly release cache resources
            self._release_cache()
            raise

    @abstractmethod
    def _initialize_cache(self):
        """Initialize the local cache resource for the file content and return the resource object."""

    @abstractmethod
    def _remote_file_exists(self) -> bool:
        """Check if the remote file exists."""

    @abstractmethod
    def _remote_file_download(self) -> Any:
        """Download the remote file and return a readable file-like object for the downloaded content."""

    @abstractmethod
    def _remote_file_upload(self, data) -> None:
        """Upload the given data from a readable file-like object to the remote file."""

    def _do_remote_file_exists(self):
        try:
            return self._remote_file_exists()
        except Exception as e:
            raise io_error(
                self.path, message="Failed to check if remote file exists."
            ) from e

    def _do_remote_file_download(self):
        try:
            return self._remote_file_download()
        except Exception as e:
            raise io_error(
                self.path, message="Failed to download remote file to cache."
            ) from e

    def _do_remote_file_upload(self, data):
        try:
            self._remote_file_upload(data)
        except Exception as e:
            raise io_error(
                self.path, message="Failed to upload cache to remote file."
            ) from e

    @abstractmethod
    @contextmanager
    def _cached_data(self):
        pass

    def _release_cache(self):
        """Release the cache resource."""
        if self._cache:
            try:
                self._cache.close()
            finally:
                self._cache = None

    def _download_remote_file_to_cache(self):
        self.log.debug("Downloading remote file to cache: path=%s", self.path)

        with self._do_remote_file_download() as src:
            while data := src.read(self.block_size):
                new_size = self._cache.tell() + len(data)
                if self.max_size and new_size > self.max_size:
                    raise os_error(OSError, errno.EFBIG, self.path)

                self._cache.write(data)

        self.log.debug(
            "Remote file downloaded to cache: path=%s, size=%s",
            self.path,
            self._cache.tell(),
        )

    def _upload_cache_to_remote_file(self):
        self.log.debug("Uploading cache to remote file: path=%s", self.path)

        with self._cached_data() as data:
            self._do_remote_file_upload(data)

        self.log.debug("Uploaded cache to remote file: path=%s", self.path)

    def close(self):
        if not self.closed:
            try:
                try:
                    self.flush(upload=True)
                finally:
                    super().close()
            finally:
                self._release_cache()

    def readable(self):
        """Return True if the file can be read from."""
        return self._readable and self._ensure_not_closed()

    def writable(self):
        """Return True if the file can be written to."""
        return self._writable and self._ensure_not_closed()

    def seekable(self):
        """Return True if the file supports random access."""
        return self._ensure_not_closed()

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from the file and return them.
        If size is negative or omitted, read all until EOF."""
        self._ensure_not_closed()

        if not self.readable():
            raise UnsupportedOperation(f"File not open for reading: path={self.path}")

        return self._cache.read(size)

    def write(self, data: bytes) -> int:
        """Write the given bytes-like object, and return the number of bytes written."""
        self._ensure_not_closed()

        if not self.writable():
            raise UnsupportedOperation(f"File not open for writing: path={self.path}")

        if self.max_size and self._cache.tell() + len(data) > self.max_size:
            raise os_error(OSError, errno.EFBIG, self.path)

        length = self._cache.write(data)
        if length:
            self._updated = True
        return length

    def flush(self, upload: bool = False):
        """Flush the write buffers of the stream if applicable.

        Parameters
        ----------
        upload : bool, optional
            Whether to upload the cached data to the remote file after flushing the cache, by default False.
            If True, the cached data will be uploaded to the remote file if there are any updates in the cache.
        """
        if not self.writable():
            return

        self._ensure_not_closed()

        if self._cache:
            self._cache.flush()

            if self._updated and upload:
                self._upload_cache_to_remote_file()
                self._updated = False

    def tell(self) -> int:
        """Return the current stream position."""
        self._ensure_not_closed()
        return self._cache.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change the stream position to the given byte offset, interpreted relative to the position
        indicated by whence, and return the new absolute position."""
        self._ensure_not_closed()
        return self._cache.seek(offset, whence)

    def truncate(self, size=None, /):
        """Resize the stream to the given size in bytes."""
        self._ensure_not_closed()
        return self._cache.truncate(size)


class MemoryCachedFile(AbstractCachedFile, ABC):
    """A cached file implementation that uses an in-memory bytes buffer as the cache."""

    def __init__(
        self,
        path: str,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )

    def _initialize_cache(self):
        if self.max_size is None:
            raise ValueError(
                "max_size must be specified for MemoryCachedFile to prevent OOM."
            )
        return BytesIO()

    @contextmanager
    def _cached_data(self):
        yield BytesIO(self._cache.getbuffer())


class FileCachedFile(AbstractCachedFile, ABC):
    """A cached file implementation that uses a temporary file on disk as the cache."""

    def __init__(
        self,
        path: str,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )

    def _initialize_cache(self):
        cache = NamedTemporaryFile(delete=False, mode="w+b")  # noqa: SIM115
        self._cache_file_name = cache.name
        return cache

    @contextmanager
    def _cached_data(self):
        pos = self._cache.tell()
        self._cache.close()

        try:
            with open(self._cache_file_name, "rb") as f:
                yield f
        finally:
            self._cache = open(self._cache_file_name, "r+b")  # noqa: SIM115
            self._cache.seek(pos)

    def _release_cache(self):
        if self._cache:
            try:
                super()._release_cache()
            finally:
                if os.path.exists(self._cache_file_name):
                    os.remove(self._cache_file_name)


def read_mode(mode: str) -> bool:
    """Return True if the given file mode allows reading from the file, False otherwise."""
    return "r" in mode or "+" in mode


def write_mode(mode: str) -> bool:
    """Return True if the given file mode allows writing to the file, False otherwise."""
    return "w" in mode or "a" in mode or "x" in mode or "+" in mode


__all__ = [
    "AbstractAsyncFile",
    "AbstractAsyncReadableFile",
    "AbstractAsyncWritableFile",
    "AbstractCachedFile",
    "FileCachedFile",
    "FileRangeTaskSupport",
    "MemoryCachedFile",
    "read_mode",
    "write_mode",
]

import asyncio
from datetime import datetime, timedelta, timezone
from io import BytesIO, UnsupportedOperation
from random import randbytes
from time import perf_counter
from typing import Any
from unittest.mock import patch

import pytest

from fsspec_databricks.file import (
    AbstractAsyncFile,
    AbstractAsyncReadableFile,
    AbstractAsyncWritableFile,
    FileCachedFile,
    FileRangeTaskSupport,
    MemoryCachedFile,
)
from fsspec_databricks.http import AioHttpClientMixin

from .utils import bytes_sig


@pytest.mark.asyncio
async def test_abstract_async_file_init():
    loop = asyncio.get_event_loop()

    async with AbstractAsyncFile(
        path="/path/to/file",
        loop=loop,
    ) as file:
        assert file.path == "/path/to/file"
        assert file._loop == loop


@pytest.mark.asyncio
async def test_abstract_async_file_close(client_event_loop):
    file1 = AbstractAsyncFile(
        path="/path/to/file",
        loop=client_event_loop,
    )
    file1.close()
    assert file1.closed
    file1.close()

    with pytest.raises(ValueError, match="I/O operation on closed file"):
        file1._ensure_not_closed()

    file2 = AbstractAsyncFile(
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
    )

    with pytest.raises(
        RuntimeError,
        match=r"Run a synchronous operation in the same event loop attached to the file object is not allowed.",
    ):
        file2.close()

    # Cleanup
    await file2.aclose()


@pytest.mark.asyncio
async def test_abstract_async_file_aclose():
    file = AbstractAsyncFile(
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
    )

    await file.aclose()
    assert file.closed
    await file.aclose()


@pytest.mark.asyncio
async def test_file_range_task_support_run_init():
    async with FileRangeTaskSupport(
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
    ) as file:
        assert file.max_concurrency == FileRangeTaskSupport.default_max_concurrency

    async with FileRangeTaskSupport(
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        max_concurrency=2,
    ) as file:
        assert file.max_concurrency == 2


@pytest.mark.asyncio
async def test_file_range_task_support_schedule_task():
    async def ok():
        return "ok"

    async def sleep():
        await asyncio.sleep(0.5)
        return "sleep"

    async def error():
        raise RuntimeError("Dummy error")

    async with FileRangeTaskSupport(
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        max_concurrency=2,
    ) as file:
        t = perf_counter()
        await file._schedule_task(0, 1, sleep)
        assert perf_counter() - t < 0.1

        t = perf_counter()
        await file._schedule_task(1, 2, sleep)
        assert perf_counter() - t < 0.1

        # Wait for semaphore to be available.
        t = perf_counter()
        await file._schedule_task(2, 3, ok)
        assert perf_counter() - t > 0.5

        t = perf_counter()
        await file._schedule_task(3, 4, sleep)
        assert perf_counter() - t < 0.1

        t = perf_counter()
        await file._schedule_task(4, 5, ok)
        assert perf_counter() - t < 0.1

        assert [(t.start, t.end) for t in file._tasks] == [
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
        ]

        assert [t.task.get_name() for t in file._tasks] == [
            "file-file-0-1",
            "file-file-1-2",
            "file-file-2-3",
            "file-file-3-4",
            "file-file-4-5",
        ]

        assert (await file._tasks[0]) == "sleep"
        assert (await file._tasks[1]) == "sleep"
        assert (await file._tasks[2]) == "ok"
        assert (await file._tasks[3]) == "sleep"
        assert (await file._tasks[4]) == "ok"

        # Test handling of errors raised in tasks. Task schedule itself is done without exception
        await file._schedule_task(5, 6, error)

        # But result of the task raises an exception
        with pytest.raises(RuntimeError, match="Dummy error"):
            await file._tasks[5]

        # And block further task scheduling due to the error
        with pytest.raises(OSError):
            await file._schedule_task(6, 7, ok)


@pytest.mark.asyncio
async def test_file_range_task_support_cancel_all():
    async def sleep():
        await asyncio.sleep(10.0)

    async with FileRangeTaskSupport(
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        max_concurrency=5,
    ) as file:
        await file._schedule_task(0, 1, sleep)
        await file._schedule_task(1, 2, sleep)
        await file._schedule_task(2, 3, sleep)
        await file._schedule_task(3, 4, sleep)
        await file._schedule_task(4, 5, sleep)

        assert all(not t.task.done() for t in file._tasks)

        tasks = list(file._tasks)

        await file._cancel_all_tasks()
        assert len(file._tasks) == 0
        assert file._task_error is None

        assert all(t.task.cancelled() for t in tasks)
        assert all(t.task.done() for t in tasks)


class DummyReadableFile(AbstractAsyncReadableFile, AioHttpClientMixin):
    def __init__(
        self,
        path: str,
        base_url: str,
        loop,
        size: int,
        max_concurrency=10,
        block_size: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            loop=loop,
            size=size,
            max_concurrency=max_concurrency,
            block_size=block_size,
            min_block_size=min_block_size,
            max_block_size=max_block_size,
            verbose_debug_log=verbose_debug_log,
        )

        self._config_session(base_url=base_url)

        self.fetched_ranges = []

    async def _fetch_range(self, start: int, end: int) -> bytes:
        self.fetched_ranges.append((start, end))
        async with self._session.get(
            f"/api/2.0/fs/files{self.path}",
            headers={"Range": f"bytes={start}-{end - 1}"},
        ) as response:
            response.raise_for_status()
            return await response.read()

    async def aclose(self):
        if not self.closed:
            try:
                await super().aclose()
            finally:
                await self._close_session()


@pytest.mark.asyncio
async def test_abstract_async_readable_file_init():
    async with DummyReadableFile(
        base_url="https:///text.api",
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        size=10 * 1024 * 1024,
    ) as file1:
        assert file1._file_size == 10 * 1024 * 1024
        assert file1.min_block_size == AbstractAsyncReadableFile.min_block_size
        assert file1.max_block_size == AbstractAsyncReadableFile.max_block_size

    async with DummyReadableFile(
        base_url="https:///text.api",
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        size=10 * 1024 * 1024,
        block_size=123 * 1024,
    ) as file2:
        assert file2.min_block_size == 123 * 1024
        assert file2.max_block_size == 123 * 1024

    async with DummyReadableFile(
        base_url="https:///text.api",
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        size=10 * 1024 * 1024,
        min_block_size=456 * 1024,
        max_block_size=789 * 1024,
    ) as file3:
        assert file3.min_block_size == 456 * 1024
        assert file3.max_block_size == 789 * 1024


@pytest.mark.asyncio
async def test_abstract_async_readable_file_read(
    dummy_api, dummy_api_context, client_event_loop
):
    size = 10 * 1024 * 1024
    block_size = 1000 * 1000
    dummy_api_context.files["/path/to/file"] = randbytes(size)

    with DummyReadableFile(
        base_url=dummy_api,
        path="/path/to/file",
        loop=client_event_loop,
        size=size,
        block_size=block_size,
        max_concurrency=5,
    ) as file:
        fetched_ranges = file.fetched_ranges

        data = file.read()
        assert bytes_sig(data) == bytes_sig(dummy_api_context.files["/path/to/file"])
        assert file.tell() == size
        fetched_ranges.sort()

        assert fetched_ranges == [
            (i, min(i + block_size, size)) for i in range(0, size, block_size)
        ]


@pytest.mark.asyncio
async def test_abstract_async_readable_file_seek(
    dummy_api, dummy_api_context, client_event_loop
):
    size = 10 * 1024 * 1024
    block_size = 1000 * 1000
    dummy_api_context.files["/path/to/file"] = randbytes(size)

    with DummyReadableFile(
        base_url=dummy_api,
        path="/path/to/file",
        loop=client_event_loop,
        size=size,
        block_size=block_size,
        max_concurrency=5,
    ) as file:
        fetched_ranges = file.fetched_ranges

        # Initial fetch
        data = file.read(1024)
        assert bytes_sig(data) == bytes_sig(
            dummy_api_context.files["/path/to/file"][:1024]
        )
        assert file.tell() == 1024
        assert fetched_ranges == [(0, block_size)]

        # Seek within the already fetched range.
        offset1 = 5 * 1024
        file.seek(offset1)
        assert file.tell() == offset1

        data = file.read(1024)
        assert (
            data == dummy_api_context.files["/path/to/file"][offset1 : offset1 + 1024]
        )
        assert file.tell() == offset1 + 1024
        assert fetched_ranges == [
            (0, block_size),
        ]

        # Seek to a different range.
        offset2 = 2 * 1024 * 1024 + 500 * 100
        file.seek(offset2)
        assert file.tell() == offset2

        data = file.read(1000 * 100)
        assert bytes_sig(data) == bytes_sig(
            dummy_api_context.files["/path/to/file"][offset2 : offset2 + 1000 * 100]
        )
        assert file.tell() == offset2 + 1000 * 100
        assert fetched_ranges == [
            (0, block_size),
            (offset2, offset2 + block_size),
        ]

        # Seek with whence=1.
        offset3 = file.tell()
        file.seek(-1024 * 1024, 1)
        offset3 -= 1024 * 1024
        assert file.tell() == offset3

        data = file.read(1000 * 100)
        assert bytes_sig(data) == bytes_sig(
            dummy_api_context.files["/path/to/file"][offset3 : offset3 + 1000 * 100]
        )
        assert fetched_ranges == [
            (0, block_size),
            (offset2, offset2 + block_size),
            (offset3, offset3 + block_size),
        ]

        # Seek with whence=2.
        file.seek(-1024, 2)
        offset4 = size - 1024
        assert file.tell() == offset4

        data = file.read(1000 * 100)
        assert bytes_sig(data) == bytes_sig(
            dummy_api_context.files["/path/to/file"][offset4:size]
        )
        assert file.tell() == size
        assert fetched_ranges == [
            (0, block_size),
            (offset2, offset2 + block_size),
            (offset3, offset3 + block_size),
            (offset4, size),
        ]

        # Invalid whence.
        with pytest.raises(ValueError, match="Invalid whence 3: path=/path/to/file"):
            file.seek(123, 3)

        # Seek before the beginning of the file.
        with pytest.raises(
            ValueError,
            match="Seek position -1 out of the file bound: path=/path/to/file",
        ):
            file.seek(-1, 0)

        # Seek after the end of the file.
        with pytest.raises(
            ValueError,
            match="Seek position 10485761 out of the file bound: path=/path/to/file",
        ):
            file.seek(1, 2)


@pytest.mark.asyncio
async def test_abstract_async_readable_file_adaptive_read(
    dummy_api, dummy_api_context, client_event_loop
):
    size = 10 * 1024 * 1024
    dummy_api_context.files["/path/to/file"] = randbytes(size)

    with DummyReadableFile(
        base_url=dummy_api,
        path="/path/to/file",
        loop=client_event_loop,
        size=size,
        max_concurrency=5,
        min_block_size=64 * 1024,
        max_block_size=1024 * 1024,
    ) as file:
        data = bytearray()
        previous_read_length = 0
        block_sizes = set()

        while chunk := file.read(64 * 1024):
            for t in file._tasks:
                block_sizes.add(t.end - t.start)
            if chunk:
                assert file._read_length > previous_read_length
                previous_read_length = file._read_length
                data.extend(chunk)
            else:
                break

        assert bytes_sig(data) == bytes_sig(dummy_api_context.files["/path/to/file"])
        assert file.tell() == size
        assert block_sizes == {
            64 * 1024,
            128 * 1024,
            256 * 1024,
            512 * 1024,
            1024 * 1024,
        }

        file.seek(123)
        assert file._read_length == 0


def expire_time() -> str:
    return (datetime.now(tz=timezone.utc) + timedelta(minutes=10)).isoformat()


class DummyWritableFile(AbstractAsyncWritableFile, AioHttpClientMixin):
    def __init__(
        self,
        path: str,
        base_url: str,
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
            min_block_size=min_block_size,
            max_block_size=max_block_size,
            min_multipart_upload_size=min_multipart_upload_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )

        self._config_session(base_url=base_url)

        self._session_token: str | None = None

    async def _upload_all(self, data: bytes) -> None:
        async with self._session.put(
            f"/api/2.0/fs/files{self.path}",
            data=data,
        ) as response:
            response.raise_for_status()

    async def _start_multipart_upload(self) -> None:
        async with self._session.post(
            f"/api/2.0/fs/files{self.path}",
            params={"action": "initiate-upload"},
        ) as response:
            response.raise_for_status()

            result = await response.json()
            self._session_token = result["multipart_upload"]["session_token"]

    async def _upload_part(
        self, data: bytes, start: int, end: int, part_index: int
    ) -> Any:
        async with self._session.put(
            f"/api/2.0/fs/files{self.path}",
            params={
                "upload_type": "multipart",
                "part_number": part_index,
                "session_token": self._session_token,
            },
            data=data,
        ) as response:
            response.raise_for_status()
            return part_index, response.headers["etag"]

    async def _complete_multipart_upload(self) -> None:
        parts = [await t for t in self._tasks]
        parts.sort()

        async with self._session.post(
            f"/api/2.0/fs/files{self.path}",
            params={
                "action": "complete-upload",
                "upload_type": "multipart",
                "session_token": self._session_token,
            },
            json={
                "session_token": self._session_token,
                "parts": [{"part_number": i, "etag": etag} for i, etag in parts],
            },
        ) as response:
            response.raise_for_status()

    async def _abort_multipart_upload(self) -> None:
        async with self._session.delete(
            f"/api/2.0/fs/files{self.path}",
            params={
                "action": "abort-upload",
                "upload_type": "multipart",
                "session_token": self._session_token,
            },
        ) as response:
            response.raise_for_status()

    async def aclose(self):
        if not self.closed:
            try:
                await super().aclose()
            finally:
                await self._close_session()


@pytest.mark.asyncio
async def test_abstract_async_writable_file_init():
    async with DummyWritableFile(
        base_url="https:///text.api",
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
    ) as file:
        assert file.min_block_size == AbstractAsyncWritableFile.min_block_size
        assert file.max_block_size == AbstractAsyncWritableFile.max_block_size

    async with DummyWritableFile(
        base_url="https:///text.api",
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        min_block_size=123 * 1024,
        max_block_size=456 * 1024,
    ) as file:
        assert file.min_block_size == 123 * 1024
        assert file.max_block_size == 456 * 1024

    async with DummyWritableFile(
        base_url="https:///text.api",
        path="/path/to/file",
        loop=asyncio.get_event_loop(),
        min_block_size=123 * 1024,
        max_block_size=789 * 1024,
        block_size=456 * 1024,
    ) as file:
        assert file.min_block_size == 456 * 1024
        assert file.max_block_size == 456 * 1024


@pytest.mark.asyncio
async def test_abstract_async_writable_file_write(
    dummy_api, dummy_api_context, client_event_loop
):
    data = randbytes(10 * 1024 * 1024)
    step = 123 * 100

    # Multipart upload
    with DummyWritableFile(
        base_url=dummy_api,
        path="/path/to/file",
        loop=client_event_loop,
        block_size=1000 * 1000,
        max_concurrency=5,
    ) as file:
        for i in range(0, len(data), step):
            file.write(data[i : min(i + step, len(data))])

        assert len(dummy_api_context.upload_sessions) == 1

    assert bytes_sig(data) == bytes_sig(dummy_api_context.files["/path/to/file"])

    # Single shot upload
    with DummyWritableFile(
        base_url=dummy_api,
        path="/path/to/file2",
        loop=client_event_loop,
        block_size=1000 * 1000,
        min_multipart_upload_size=20 * 1024 * 1024,
        max_concurrency=5,
    ) as file:
        for i in range(0, len(data), step):
            file.write(data[i : min(i + step, len(data))])

        assert len(dummy_api_context.upload_sessions) == 0

    assert bytes_sig(data) == bytes_sig(dummy_api_context.files["/path/to/file2"])


@pytest.mark.asyncio
async def test_abstract_async_writable_file_write_with_errors(
    dummy_api, dummy_api_context, client_event_loop
):
    data = randbytes(10 * 1024 * 1024)
    step = 123 * 100

    with patch.object(
        DummyWritableFile,
        "_start_multipart_upload",
        side_effect=RuntimeError("Dummy"),
    ):
        with pytest.raises(OSError, match=r"Failed to start upload.") as exc_info:
            with DummyWritableFile(
                base_url=dummy_api,
                path="/path/to/file",
                loop=client_event_loop,
                block_size=1000 * 1000,
                max_concurrency=5,
            ) as file:
                for i in range(0, len(data), step):
                    file.write(data[i : min(i + step, len(data))])

    org_exc = exc_info.value.__cause__
    assert isinstance(org_exc, RuntimeError)
    assert org_exc.args == ("Dummy",)

    assert not file._multipart_uploading
    assert not dummy_api_context.files
    assert not dummy_api_context.upload_sessions

    with patch.object(
        DummyWritableFile,
        "_upload_part",
        side_effect=RuntimeError("Dummy"),
    ):
        with pytest.raises(OSError, match="Multipart upload failed and was aborted"):
            with DummyWritableFile(
                base_url=dummy_api,
                path="/path/to/file",
                loop=client_event_loop,
                block_size=1000 * 1000,
                max_concurrency=5,
            ) as file:
                with pytest.raises(OSError) as exc_info:
                    for i in range(0, len(data), step):
                        file.write(data[i : min(i + step, len(data))])

    org_exc = exc_info.value.__cause__
    assert isinstance(org_exc, RuntimeError)
    assert org_exc.args == ("Dummy",)

    assert not file._multipart_uploading
    assert not dummy_api_context.files
    assert not dummy_api_context.upload_sessions

    with patch.object(
        DummyWritableFile,
        "_complete_multipart_upload",
        side_effect=RuntimeError("Dummy"),
    ):
        with pytest.raises(OSError, match=r"Failed to complete upload.") as exc_info:
            with DummyWritableFile(
                base_url=dummy_api,
                path="/path/to/file",
                loop=client_event_loop,
                block_size=1000 * 1000,
                max_concurrency=5,
            ) as file:
                for i in range(0, len(data), step):
                    file.write(data[i : min(i + step, len(data))])

    org_exc = exc_info.value.__cause__
    assert isinstance(org_exc, RuntimeError)
    assert org_exc.args == ("Dummy",)

    assert not file._multipart_uploading
    assert not dummy_api_context.files
    assert not dummy_api_context.upload_sessions

    with patch.object(
        DummyWritableFile,
        "_upload_part",
        side_effect=RuntimeError("Dummy1"),
    ):
        with patch.object(
            DummyWritableFile,
            "_abort_multipart_upload",
            side_effect=RuntimeError("Dummy2"),
        ):
            with pytest.raises(OSError, match=r"Failed to abort upload") as exc_info:
                with DummyWritableFile(
                    base_url=dummy_api,
                    path="/path/to/file",
                    loop=client_event_loop,
                    block_size=1000 * 1000,
                    max_concurrency=5,
                ) as file:
                    with pytest.raises(
                        OSError,
                        match=r"blocked due to a previous error during file access.",
                    ):
                        for i in range(0, len(data), step):
                            file.write(data[i : min(i + step, len(data))])

    org_exc = exc_info.value.__cause__
    assert isinstance(org_exc, RuntimeError)
    assert org_exc.args == ("Dummy2",)

    assert not file._multipart_uploading
    assert not dummy_api_context.files
    # Abort of the file upload failed -> Session remains
    assert dummy_api_context.upload_sessions


class DummyCachedFileMixin:
    _remote_file: bytes | None = None

    def _remote_file_exists(self):
        return self._remote_file is not None

    def _remote_file_download(self):
        return BytesIO(self._remote_file)

    def _remote_file_upload(self, data):
        self._remote_file = data.read()


class DummyMemoryCachedFile(DummyCachedFileMixin, MemoryCachedFile):
    def __init__(
        self,
        remote_file: bytes | None = None,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        self._remote_file = remote_file

        super().__init__(
            path="/path/to/file",
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )


class DummyFileCachedFile(DummyCachedFileMixin, FileCachedFile):
    def __init__(
        self,
        remote_file: bytes | None = None,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        self._remote_file = remote_file

        super().__init__(
            path="/path/to/file",
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_memory_cached_file_init(file_class):
    data = randbytes(10 * 1024 * 1024)

    with (
        pytest.raises(FileNotFoundError),
        file_class(
            remote_file=None,
            mode="rb",
            max_size=12 * 1024 * 1024,
            block_size=3456 * 1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="rb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "rb"
        assert file.max_size == 12 * 1024 * 1024
        assert file.block_size == 3456 * 1024

        assert file.readable()
        assert not file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == data

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "wb"

        assert not file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=data,
        mode="wb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=None,
        mode="ab",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "ab"

        assert not file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=data,
        mode="ab",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.tell() == len(data)

        file._cache.seek(0)
        assert file._cache.read() == data

    with file_class(
        remote_file=None,
        mode="xb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "xb"

        assert not file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with (
        pytest.raises(FileExistsError),
        file_class(
            remote_file=data,
            mode="xb",
            max_size=12 * 1024 * 1024,
            block_size=3456 * 1024,
        ) as _,
    ):
        pass

    with (
        pytest.raises(FileNotFoundError),
        file_class(
            remote_file=None,
            mode="r+b",
            max_size=12 * 1024 * 1024,
            block_size=3456 * 1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="r+b",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "r+b"
        assert file.max_size == 12 * 1024 * 1024
        assert file.block_size == 3456 * 1024

        assert file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == data

    with file_class(
        remote_file=None,
        mode="w+b",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "w+b"

        assert file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.tell() == 0
        assert file._cache.read() == b""


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_read(file_class):
    data = randbytes(1024 * 1024)

    with file_class(
        remote_file=data,
        mode="rb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert bytes_sig(file.read()) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="rb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        data2 = bytearray()
        while chunk := file.read(3456 * 1024):
            data2.extend(chunk)
        assert bytes_sig(data2) == bytes_sig(data)

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=data,
            mode="wb",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.read()

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=data,
            mode="ab",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.read()

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=None,
            mode="xb",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.read()

    with file_class(
        remote_file=data,
        mode="r+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert bytes_sig(file.read()) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert file.read() == b""


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_write(file_class):
    data = randbytes(1024 * 1024)

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=data,
            mode="rb",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.write(b"test")

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        for i in range(0, len(data), 123 * 100):
            file.write(data[i : min(i + 123 * 100, len(data))])

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="ab",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data + data)

    with file_class(
        remote_file=None,
        mode="xb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="r+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_flush(file_class):
    data = randbytes(1024 * 1024)
    chunk = randbytes(256)

    with file_class(
        remote_file=data,
        mode="wb",
        max_size=12 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert bytes_sig(file._remote_file) == bytes_sig(data)

        file.write(chunk)
        assert bytes_sig(file._remote_file) == bytes_sig(data)

        file.flush()
        assert bytes_sig(file._remote_file) == bytes_sig(data)

        file.flush(upload=True)
        assert bytes_sig(file._remote_file) == bytes_sig(chunk)

        file.flush()
        assert bytes_sig(file._remote_file) == bytes_sig(chunk)

        file.write(chunk)
        assert bytes_sig(file._remote_file) == bytes_sig(chunk)

        file.flush(upload=True)
        assert bytes_sig(file._remote_file) == bytes_sig(chunk + chunk)

    assert bytes_sig(file._remote_file) == bytes_sig(chunk + chunk)


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_max_init_size(file_class):
    data = randbytes(1024 * 1024)

    with (
        pytest.raises(OSError),
        file_class(
            remote_file=data,
            mode="rb",
            max_size=1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="wb",
        max_size=1024,
    ) as _:
        pass

    with (
        pytest.raises(OSError),
        file_class(
            remote_file=data,
            mode="ab",
            max_size=1024,
        ) as _,
    ):
        pass

    with (
        pytest.raises(OSError),
        file_class(
            remote_file=data,
            mode="r+b",
            max_size=1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=1024,
    ) as _:
        pass


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_max_write_size(file_class):
    data = randbytes(1024 * 1024)
    chunk = randbytes(256)

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=256,
    ) as file:
        file.write(chunk)

        with pytest.raises(OSError):
            file.write(chunk)

    with file_class(
        remote_file=data,
        mode="ab",
        max_size=len(data) + 256,
    ) as file:
        file.write(chunk)

        with pytest.raises(OSError):
            file.write(chunk)


def test_memory_cached_file_max_size_check():
    with (
        pytest.raises(
            ValueError,
            match=r"max_size must be specified for MemoryCachedFile to prevent OOM.",
        ),
        DummyMemoryCachedFile(
            remote_file=None,
            mode="wb",
            # No max_size
        ) as _,
    ):
        pass

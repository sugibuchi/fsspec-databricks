# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""File objects whose requests adapt to how a file is read or written.

For storages reading byte ranges and writing in parts:

- `AdaptiveReadableFile` fetches small blocks for scattered reads. The longer
  a run of sequential reads, the bigger its blocks, and the further ahead it
  fetches them, several at a time.
- `AdaptiveWritableFile` uploads small files in one request, and larger ones
  in parts that grow with the bytes written, several at a time.

This module depends on `_async_file`.
"""

from __future__ import annotations

import asyncio
import errno
import io
import math
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any

from ._async_file import _AsyncFile, _MiB, _Range

__all__ = ["AdaptiveReadableFile", "AdaptiveWritableFile"]


class _AdaptiveFile(_AsyncFile):
    """A file splitting large reads or writes into requests that run at once.

    A large read or write is split into ``spread`` requests, which run at once
    as far as the concurrency limit allows. So the limit also bounds the memory
    of the parts in flight. A file given no limit runs ``spread`` requests at a
    time, the number it plans for.

    Its parameters are the subclasses' first ones: those of `_AsyncFile`, and
    ``spread``.
    """

    spread: int = 8
    """How many requests a large read or write is split into, to run at once."""

    min_block_size: int
    max_block_size: int

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        concurrency_limit: asyncio.Semaphore | None = None,
        spread: int | None = None,
    ) -> None:
        if spread is not None and spread < 1:
            raise ValueError(f"spread must be at least 1: {spread}")

        self.spread = spread or self.spread
        if concurrency_limit is None:
            concurrency_limit = asyncio.Semaphore(self.spread)

        super().__init__(path, loop, concurrency_limit=concurrency_limit)

    def _block_size(self, target: int) -> int:
        """The block size for ``target`` bytes, in ``spread`` requests at most.

        It is ``min_block_size`` times a power of 2, so that blocks stay
        aligned, and at most ``max_block_size``. A subclass may override it for
        a storage's own rule, e.g. rounding to the multiple of a size it needs.
        A writer still sends every part but the last at least
        ``min_block_size``, with the last taking the rest.
        """
        smallest = self.min_block_size
        needed = math.ceil(target / self.spread)
        if needed <= smallest:
            return smallest

        doublings = (math.ceil(needed / smallest) - 1).bit_length()
        return min(smallest << doublings, self.max_block_size)


class AdaptiveReadableFile(_AdaptiveFile, ABC):
    """A file read through blocks fetched as its reads show how it is read.

    It assumes a storage where a request's latency outweighs the bytes it
    returns, up to a block, as with object stores. Each fetch is one
    ``_fetch_range`` request, which a subclass implements. Requests cost too,
    so it sends as few as the reads allow:

    - A read fetches what it asks for. From twice ``split_size``, it is split
      into even parts of at least that, up to ``spread`` of them, fetched
      together: one request returns its bytes at a bounded rate, e.g. about 70
      MiB/s on Azure, where several in parallel go faster. No part exceeds
      ``max_block_size``.
    - A read smaller than ``min_block_size`` fetches the block of that size
      holding it, aligned to a multiple of it. So reads near each other share
      it, e.g. a Parquet file's footer and its metadata. Keeping it small
      keeps scattered small reads cheap.
    - A read that starts where the last one ended continues a run. A run that
      has read n bytes is assumed to read n more: they are fetched ahead,
      topped up once half of them are read. They are split into ``spread``
      blocks, fetched at once as the concurrency limit allows. Their blocks
      double in size up to ``max_block_size``, so at most ``spread`` times
      ``max_block_size`` bytes are fetched ahead.
    - A seek within the blocks fetched keeps them and the run. So does a seek
      forward past them by at most ``max_gap``, and by less than the run has
      read: the bytes skipped are not fetched, and the run goes on fetching
      ahead. A seek elsewhere cancels the blocks in flight and ends the run,
      so that scattered reads, such as a Parquet file's column chunks, fetch
      nothing ahead.
    - Fetched blocks are kept, up to ``kept_size`` bytes, for later reads that
      come back to them.

    It wins over fsspec's caches when the reads are not known in advance, or
    mix both kinds. It loses on storages whose requests are cheap, where
    fetching a block for a few bytes reads more than needed.

    Its many requests read one version of the file, even if another client
    replaces it meanwhile: the blocks of two versions never mix. A version is
    what the storage names one with: an ETag, or a version id.

    - ``version`` is the version to read, e.g. from the filesystem's
      ``_info``. If None, the first response decides it.
    - `_fetch_range` asks for ``version`` when the storage can, e.g. with
      If-Match for an ETag, or by its version id. The storage then refuses, or
      never sends, another version's bytes.
    - `_fetch_range` passes the version of every response to
      `_check_version`, which raises OSError with ``errno.ESTALE`` for another
      one. This is needed even where the storage can ask for a version: the
      first fetches run at once, before any version is known.

    An ETag names the file's content, so a read by If-Match fails once another
    client replaces the file. A version id names a version the storage keeps,
    e.g. in a versioned S3 bucket: the read goes on with it.

    A storage whose responses name no version may still tell one apart, e.g.
    by the modification time and size its status gives:

    - ``version`` is then that token, from the info the file was opened with.
    - `_current_version` returns the token as the storage has it now, which
      each fetch checks once its bytes arrive.
    - Checked after the bytes arrive, a token unchanged shows they came from
      that version. This detects a file replaced while read, without
      preventing it: the storage cannot refuse another version's bytes. It
      costs a request per fetch, and is only as fine as the token, e.g. two
      writes within a modification time's resolution look alike.

    Parameters
    ----------
    path : str
        The file's path, for messages.
    loop : asyncio.AbstractEventLoop
        The loop the fetches run on, e.g. the filesystem's ``loop``.
    size : int
        The file's size, e.g. from the filesystem's ``_info``.
    concurrency_limit : asyncio.Semaphore, optional
        Held by each fetch while it runs. Share one between files to limit
        their fetches together, e.g. per filesystem. The file's own, of
        ``spread``, if None.
    spread : int, optional
        How many blocks a large read or a readahead is split into, to fetch at
        once. The class attribute if None.
    min_block_size, max_block_size : int, optional
        The smallest and largest request. The class attributes if None.
    split_size : int, optional
        The smallest part a read is split into. The class attribute if None.
    max_gap : int, optional
        The longest forward seek past the blocks fetched that keeps the run,
        if shorter than what the run has read. The class attribute if None,
        ``min_block_size`` if that is None too.
    kept_size : int, optional
        The most bytes of fetched blocks kept. The class attribute if None.
    version : str, optional
        The version of the file to read. The first response's if None.

    Raises
    ------
    ValueError
        If ``spread`` is less than 1.

    See Also
    --------
    AdaptiveWritableFile : Its counterpart for writing.

    Examples
    --------
    >>> limit = asyncio.Semaphore(32)  # For all the files of a filesystem.
    >>> class MyFile(AdaptiveReadableFile):
    ...     def __init__(self, fs, path, size):
    ...         super().__init__(path, fs.loop, size, concurrency_limit=limit)
    ...         self.fs = fs
    ...
    ...     async def _fetch_range(self, start, end):
    ...         return await self.fs._cat_file(self.path, start=start, end=end)

    Reading one version, on a storage reading by ETag:

    >>> class MyVersionedFile(AdaptiveReadableFile):
    ...     async def _fetch_range(self, start, end):
    ...         data, etag = await self.client.get(
    ...             self.path, start, end, if_match=self.version
    ...         )
    ...         self._check_version(etag)
    ...         return data
    """

    min_block_size: int = 64 * 1024
    """The smallest request, which smaller reads fetch whole.

    64 KiB holds a Parquet file's footer and its metadata in most files, as
    Parquet readers ask for, and keeps a small random read from fetching much
    more than it asks.
    """

    max_block_size: int = 8 * _MiB
    """The largest request. Larger reads are split, to fetch the parts together."""

    split_size: int = 4 * _MiB
    """The smallest part a read is split into: a read below twice it is one request.

    Splitting fetches faster, but sends more requests, which storages count and
    bill: e.g. an 8 MiB column chunk is two requests.
    """

    max_gap: int | None = None
    """The longest forward seek past the blocks fetched that keeps the run.

    None, the default, makes it ``min_block_size``.
    """

    kept_size: int = 8 * _MiB
    """The most bytes of fetched blocks kept for reads that come back to them."""

    mode: str = "rb"
    """The mode, as fsspec's files have it: only reading."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        size: int,
        *,
        concurrency_limit: asyncio.Semaphore | None = None,
        spread: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        split_size: int | None = None,
        max_gap: int | None = None,
        kept_size: int | None = None,
        version: str | None = None,
    ) -> None:
        super().__init__(path, loop, concurrency_limit=concurrency_limit, spread=spread)

        self.size = size
        self.version = version
        self.min_block_size = min_block_size or self.min_block_size
        self.max_block_size = max(
            max_block_size or self.max_block_size, self.min_block_size
        )
        self.split_size = split_size or self.split_size
        if max_gap is not None:
            self.max_gap = max_gap
        elif self.max_gap is None:
            self.max_gap = self.min_block_size
        self.kept_size = self.kept_size if kept_size is None else kept_size

        self._pos = 0
        # Where the blocks fetched were last settled: seeks only move _pos.
        self._settled = 0
        # The bytes read in sequence since the last seek away.
        self._run = 0
        # Blocks no longer ahead of the position, by start, the least recently
        # used first.
        self._kept: OrderedDict[int, tuple[int, bytes]] = OrderedDict()
        self._kept_bytes = 0

    # The file's API: io's methods, each with its async counterpart.

    def readable(self) -> bool:
        self._check_open()
        return True

    def seekable(self) -> bool:
        self._check_open()
        return True

    def read(self, size: int | None = -1) -> bytes:
        self._check_open()
        return self._sync(self._read(-1 if size is None else size))

    def readall(self) -> bytes:
        return self.read(-1)

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(memoryview(buffer)))
        memoryview(buffer).cast("B")[: len(data)] = data
        return len(data)

    async def _read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes, or to the end if negative, as ``read`` does."""
        self._check_open()
        await self._settle()
        pos = self._pos
        if size == 0 or pos >= self.size:
            return b""

        end = self.size if size < 0 else min(pos + size, self.size)
        if not self._requests.queue and (data := self._from_kept(pos, end)) is not None:
            self._advance(end)
            return data

        await self._fetch(pos, end)
        try:
            data = await self._collect(pos, end)
        except Exception:
            # The next read fetches again, rather than meeting the same failure.
            await self._drop_all()
            raise

        self._advance(end)
        return data

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        # Only moves: the next read drops the blocks the seek left behind. So
        # seeking sends nothing, and awaits nothing, as in fsspec's async files.
        self._check_open()
        if whence == io.SEEK_SET:
            pos = offset
        elif whence == io.SEEK_CUR:
            pos = self._pos + offset
        elif whence == io.SEEK_END:
            pos = self.size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        if pos < 0:
            raise ValueError(f"Negative seek position: {pos}")

        self._pos = pos
        return pos

    def tell(self) -> int:
        self._check_open()
        return self._pos

    # What a subclass implements, and calls.

    @abstractmethod
    async def _fetch_range(self, start: int, end: int) -> bytes:
        """Fetch the bytes [start, end) of the file in one request.

        It asks for ``version`` if the storage can, and passes the version the
        response came from to `_check_version` if the storage tells it.
        """

    async def _current_version(self) -> str | None:
        """The file's version now, for a storage whose responses name none.

        Each fetch awaits it after `_fetch_range`, and passes what it returns to
        `_check_version`. A subclass returns a token, e.g. the modification time
        and size from the filesystem's ``_info``, as ``version`` was given. None,
        the default, checks nothing and sends no request.
        """
        return None

    async def _fetch_checked(self, start: int, end: int) -> bytes:
        """Fetch [start, end), then check the version the file has after it."""
        data = await self._fetch_range(start, end)
        if (version := await self._current_version()) is not None:
            self._check_version(version)
        return data

    def _check_version(self, version: str) -> None:
        """Check the version a response came from, e.g. its ETag.

        `_fetch_range` calls it for every response. The first call, when no
        ``version`` was given, sets it: fetches run at once, so it is the first
        response to arrive. Any later response from another version raises
        OSError with ``errno.ESTALE``, so that the read never mixes two.
        """
        if self.version is None:
            self.version = version
        elif version != self.version:
            raise OSError(
                errno.ESTALE,
                f"Changed while read, from version {self.version} to {version}",
                self.path,
            )

    # Finishing and aborting, for _AsyncFile.
    #
    # A closed file holds no data, even while something still refers to it:
    # its blocks may weigh up to kept_size, and their fetches are cancelled by
    # closing anyway.

    async def _finish(self) -> None:
        self._release()

    async def _abort(self) -> None:
        self._release()
        await super()._abort()

    def _release(self) -> None:
        self._kept.clear()
        self._kept_bytes = 0
        self._run = 0

    # The reading underneath: blocks fetched ahead, kept, and dropped.

    async def _settle(self) -> None:
        """Drop the blocks the seeks since the last read left behind."""
        pos = self._pos
        if pos == self._settled:
            return

        last, self._settled = self._settled, pos
        queue = self._requests.queue
        fetched = queue[-1].end if queue else last
        if queue and queue[0].start <= pos <= queue[-1].end:
            # Many readers seek before each read, even in sequence: within the
            # blocks fetched, or right after them, the run goes on.
            behind = []
            while queue and queue[0].end <= pos:
                behind.append(queue.popleft())
            await self._drop(behind)
        elif last < pos and pos - fetched <= min(self.max_gap, self._run - 1):
            # A few bytes skipped, fewer than the run has read: still reading in
            # sequence, the run goes on fetching ahead, but the bytes skipped
            # are not fetched. Scattered small reads skip more than they read,
            # and fetch nothing ahead.
            run = self._run
            await self._drop_all()
            self._run = run
        else:
            await self._drop_all()

    def _advance(self, end: int) -> None:
        self._run += end - self._pos
        self._pos = self._settled = end

    async def _fetch(self, pos: int, end: int) -> None:
        """Send the fetches for [pos, end), and those ahead of it a run calls for."""
        queue = self._requests.queue
        continuing = self._run > 0
        if not queue and end - pos < self.min_block_size:
            # Aligned, so that reads near each other share the block, and the
            # kept ones serve later reads.
            start = pos - pos % self.min_block_size
            await self._send(start, max(end, start + self.min_block_size))
        elif (start := queue[-1].end if queue else pos) < end:
            # Even parts of at least split_size, up to spread of them.
            parts = max(1, min(self.spread, (end - start) // self.split_size))
            await self._send(start, end, math.ceil((end - start) / parts))

        if not continuing:
            return

        # A run that has read n bytes is assumed to read n more. Topping them up
        # only once half are read keeps the requests few and large. Fetching at
        # most a batch of the largest blocks bounds the memory of those unread.
        ahead = min(self._run + end - pos, self.spread * self.max_block_size)
        start = queue[-1].end
        if start - end < ahead // 2:
            await self._send(start, end + ahead, self._block_size(ahead))

    async def _send(self, start: int, stop: int, largest: int | None = None) -> None:
        """Fetch [start, stop), within the file, in even parts of at most ``largest``, or ``max_block_size``."""
        stop = min(stop, self.size)
        if stop <= start:
            return

        largest = min(largest or self.max_block_size, self.max_block_size)
        parts = math.ceil((stop - start) / largest)
        part = math.ceil((stop - start) / parts)
        for s in range(start, stop, part):
            e = min(s + part, stop)
            await self._requests.send(s, e, self._fetch_checked, s, e)

    async def _collect(self, pos: int, end: int) -> bytes:
        """The bytes [pos, end) from the blocks fetched, which must cover them."""
        queue = self._requests.queue
        data = bytearray()
        used = 0

        # The queue changes only once all is collected, so that a cancelled read
        # leaves it as it was.
        for r in list(queue):
            if pos >= end:
                break
            chunk = await r.task
            data += memoryview(chunk)[pos - r.start : end - r.start]
            pos = min(r.end, end)
            used += r.end <= end

        for _ in range(used):
            self._keep(queue.popleft())

        return bytes(data)

    async def _drop_all(self) -> None:
        """Drop every block ahead, and end the run."""
        ranges = list(self._requests.queue)
        self._requests.queue.clear()
        self._run = 0
        await self._drop(ranges)

    async def _drop(self, ranges: list[_Range]) -> None:
        """Keep the blocks fetched among these, and cancel the others."""
        for r in ranges:
            self._keep(r)
        await self._requests.cancel([r for r in ranges if not r.task.done()])

    def _keep(self, r: _Range) -> None:
        if not r.succeeded() or r.end - r.start > self.kept_size:
            return

        if (old := self._kept.pop(r.start, None)) is not None:
            self._kept_bytes -= len(old[1])

        data = r.task.result()
        self._kept[r.start] = r.end, data
        self._kept_bytes += len(data)

        while self._kept_bytes > self.kept_size:
            _, (_, evicted) = self._kept.popitem(last=False)
            self._kept_bytes -= len(evicted)

    def _from_kept(self, pos: int, end: int) -> bytes | None:
        """The bytes [pos, end) from one kept block, if one holds them all."""
        for start, (stop, data) in self._kept.items():
            if start <= pos and end <= stop:
                self._kept.move_to_end(start)
                return data[pos - start : end - start]
        return None


class AdaptiveWritableFile(_AdaptiveFile, ABC):
    """A file written in one upload, or in parts sized as its writes show.

    A subclass implements the storage's requests: one upload for small files,
    and a multipart upload's start, parts, completion and abort.

    - Writes are buffered up to ``min_multipart_size``. A file closed before
      any part was sent goes in one request if it is below that, no bigger
      than ``min_block_size``, or would go as one part, e.g. with ``spread`` 1.
    - Beyond, parts are uploaded as they fill, while writing goes on. A file
      with n bytes written is assumed to get n more: its parts grow to split
      that into ``spread``, from ``min_block_size`` up to ``max_block_size``.
    - Every part but the last has at least ``min_block_size`` bytes, as
      storages such as S3 require. So ``flush`` uploads nothing.
    - A write waits while the concurrency limit allows no upload. So a file
      holds at most one part in flight per permit, and one part being filled.
    - Files sharing a limit share that bound. A filesystem's ``io_limit``
      bounds all its writes in flight to its permits times ``max_block_size``,
      e.g. 16 times 16 MiB, plus a part being filled per file. A smaller limit
      or ``max_block_size`` lowers it.
    - ``flush(force=True)`` finishes the file as closing does, as fsspec's
      files do. Closing then sends nothing more, and writing raises ValueError.
    - A part that fails stops the writes, and closing aborts the upload. Either
      raises OSError, its cause the part's error. Any failure or cancellation
      while closing or force-flushing aborts too, as does a failed start.
    - Leaving a ``with`` block by an exception sends no more data: a small
      file is never sent, and a multipart upload is aborted. The garbage
      collector does the same with a file it finds open.
    - An abort failing too adds a note to the error raised, never replacing it.

    What a failed write leaves at the path is the storage's: the file only
    decides which requests to send, and when. On a storage where an upload is
    invisible until the request completing it, such as S3, Google Cloud
    Storage or Azure, the file is written whole, or not at all, and a failed
    write leaves the file that was there. On one where a file is visible while
    it is written, e.g. DBFS's create, add-block and close, a failed write may
    leave part of it, and the earlier file gone. A subclass's docstring says
    which its storage is.

    Some storages take parts only in order, one at a time, e.g. Google Cloud
    Storage's resumable uploads, where each part but the last is a multiple of
    256 KiB:

    - ``sequential`` sends a part only once the one before it is uploaded.
      Waiting, it holds no permit of the concurrency limit, which files may
      share. ``spread`` of 1 then suits best: fewer, larger parts.
    - A subclass may set ``sequential`` in ``_start_multipart_upload``, e.g.
      where the storage chooses the kind of upload when it starts. It holds
      for every part, as no part is sent before the upload starts.
    - Every part but the last has the size ``_block_size`` returns. A subclass
      overrides it to keep parts a multiple of 256 KiB, or any other rule.
    - The last part is the one given ``last``: with the file's size, as its
      ``end``. A storage where it makes the file visible completes nothing.

    Mode "xb" creates the file only if none is at the path, and
    ``exclusive`` is then True. The subclass checks it with the storage's
    means, raising FileExistsError if a file is there:

    - best, by a conditional write in the request creating the file, e.g.
      S3's or Azure's ``If-None-Match: *`` on the completion, or a create
      refusing an existing path, so that no other writer can slip in;
    - where the storage has none, by asking first, e.g. on opening, which
      races with another writer. Its docstring then says so.

    The upload is aborted after a refusal, as after any failure.

    Some storages limit each request's size, e.g. to 1 MiB. ``max_request_size``
    declares it, and no ``_upload_all`` or ``_upload_part`` call exceeds it:

    - A file bigger than it is uploaded in parts, even below
      ``min_multipart_size``.
    - Parts are at most that size. A last part that would exceed it, as it
      takes up to ``min_block_size`` more than the others, is split in two.

    Parameters
    ----------
    path : str
        The file's path, for messages.
    loop : asyncio.AbstractEventLoop
        The loop the uploads run on, e.g. the filesystem's ``loop``.
    mode : {"wb", "xb"}, default "wb"
        Whether the file replaces one at the path, or is created only if
        none is there.
    concurrency_limit : asyncio.Semaphore, optional
        Held by each upload while it runs. Share one between files to limit
        their uploads, and so their memory, together. The file's own, of
        ``spread``, if None.
    spread : int, optional
        How many parts the bytes written so far are split into, sizing the next
        parts. The class attribute if None.
    min_block_size, max_block_size : int, optional
        The smallest and largest part. The class attributes if None.
    min_multipart_size : int, optional
        The size from which a multipart upload is used. The class attribute if
        None.
    sequential : bool, optional
        Whether parts are uploaded one at a time, in order. The class attribute
        if None.
    max_request_size : int, optional
        The most bytes a request may upload. The class attribute if None, which
        is unlimited by default.

    Raises
    ------
    ValueError
        If ``mode`` is neither "wb" nor "xb", ``spread`` is less than 1, or
        ``max_request_size`` less than ``min_block_size``.

    See Also
    --------
    AdaptiveReadableFile : Its counterpart for reading.
    """

    min_block_size: int = 5 * _MiB
    """The smallest part but the last, the least S3 accepts."""

    max_block_size: int = 16 * _MiB
    """The largest part, but the last may have up to ``min_block_size`` more."""

    min_multipart_size: int = 5 * _MiB
    """The size from which a file is uploaded in parts."""

    sequential: bool = False
    """Whether parts are uploaded one at a time, in order, as some storages need."""

    max_request_size: int | None = None
    """The most bytes a request may upload, if the storage limits it."""

    mode: str = "wb"
    """The mode, as fsspec's files have it: "wb", or "xb" to create only."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        mode: str = "wb",
        concurrency_limit: asyncio.Semaphore | None = None,
        spread: int | None = None,
        min_block_size: int | None = None,
        max_block_size: int | None = None,
        min_multipart_size: int | None = None,
        sequential: bool | None = None,
        max_request_size: int | None = None,
    ) -> None:
        if mode not in ("wb", "xb"):
            raise ValueError(f"mode must be 'wb' or 'xb': {mode!r}")

        super().__init__(path, loop, concurrency_limit=concurrency_limit, spread=spread)

        self.mode = mode
        self.min_block_size = min_block_size or self.min_block_size
        self.max_block_size = max(
            max_block_size or self.max_block_size, self.min_block_size
        )
        self.min_multipart_size = min_multipart_size or self.min_multipart_size
        self.sequential = self.sequential if sequential is None else sequential
        self.max_request_size = max_request_size or self.max_request_size

        if (cap := self.max_request_size) is not None and cap < self.min_block_size:
            raise ValueError(
                f"max_request_size {cap} is less than min_block_size "
                f"{self.min_block_size}: no part would fit"
            )

        self._pos = 0
        self._buffer = bytearray()
        self._multipart = False

    @property
    def exclusive(self) -> bool:
        """Whether the file is created only if none is at the path: mode "xb"."""
        return self.mode == "xb"

    # Leaving a with block by an exception aborts, as the garbage collector
    # does: no more data is sent.

    def __exit__(
        self, exc_type: object, exc: BaseException | None, *rest: object
    ) -> None:
        if exc is None or self.closed:
            self.close()
        else:
            self._sync(self._aexit_by(exc))

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, *rest: object
    ) -> None:
        if exc is None or self.closed:
            await self.aclose()
        else:
            await self._aexit_by(exc)

    # The file's API: io's methods, each with its async counterpart.

    def writable(self) -> bool:
        self._check_open()
        return True

    def write(self, data: Any) -> int:
        self._check_open()
        self._check_not_forced()
        return self._sync(self._write(data))

    async def _write(self, data: Any) -> int:
        """Write bytes, as ``write`` does."""
        self._check_open()
        self._check_not_forced()
        self._raise_failed_upload()

        size = len(memoryview(data).cast("B"))
        self._buffer += data
        self._pos += size
        await self._upload_buffer(closing=False)
        return size

    def tell(self) -> int:
        self._check_open()
        return self._pos

    # What a subclass implements: the storage's requests.

    @abstractmethod
    async def _upload_all(self, data: bytes) -> None:
        """Upload the whole file in one request.

        With ``exclusive``, it creates the file only if none is at the path,
        as the storage allows: else it raises FileExistsError.
        """

    @abstractmethod
    async def _start_multipart_upload(self) -> None:
        """Start a multipart upload of the file."""

    @abstractmethod
    async def _upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> Any:
        """Upload the part [start, end) of the file, the ``index``th from 0.

        Parts may be uploaded out of order. What this returns, such as an ETag,
        is passed on to ``_complete_multipart_upload``.
        """

    @abstractmethod
    async def _complete_multipart_upload(self, parts: list[Any]) -> None:
        """Commit the parts uploaded, given what each upload returned, in order.

        With ``exclusive``, where the storage checks it on completion, it
        raises FileExistsError if a file is at the path.
        """

    @abstractmethod
    async def _abort_multipart_upload(self) -> None:
        """Clean up an unfinished upload, as far as the storage allows: best effort.

        Where the storage has a request discarding an unfinished upload, e.g.
        S3's AbortMultipartUpload, the abort sends it. Where it has none, the
        abort sends nothing, and leaves it to the storage, e.g. Azure's
        uncommitted blocks, collected a week after the last block. Where the
        upload is invisible until completed, an abort never deletes or
        replaces a file already at the path. It may be called after
        ``_start_multipart_upload`` failed, with nothing to clean up.
        """

    # Finishing and aborting, for _AsyncFile.

    async def _finish(self) -> None:
        try:
            self._raise_failed_upload()
            await self._upload_buffer(closing=True)
            if self._multipart:
                try:
                    parts = await asyncio.gather(
                        *(r.task for r in self._requests.queue)
                    )
                except Exception:
                    # A part failing now raises as one failing earlier does.
                    self._raise_failed_upload()
                    raise
                await self._complete_multipart_upload(list(parts))
        except BaseException as error:
            if self._multipart:
                await self._cancel_multipart_upload(error)
            raise
        finally:
            self._buffer = bytearray()

    async def _abort(self) -> None:
        self._buffer = bytearray()
        await super()._abort()
        if self._multipart:
            await self._abort_multipart_upload()

    async def _cancel_multipart_upload(self, cause: BaseException) -> None:
        """Cancel the parts in flight, then abort the upload, after ``cause``.

        The caller raises ``cause``: an abort failing only adds a note to it.
        """
        await self._requests.cancel_all()
        try:
            await self._abort_multipart_upload()
        except Exception as abort_error:
            # The cause is what the caller acts on: the abort's failure only
            # adds that parts may stay on the storage.
            cause.add_note(
                f"Aborting the upload of {self.path!r} failed: {abort_error!r}"
            )

    async def _aexit_by(self, exc: BaseException) -> None:
        """Close without finishing, as an exception leaves the with block."""
        try:
            if not self._forced:
                # A forced flush finished the file: nothing is left to undo.
                self._buffer = bytearray()
                if self._multipart:
                    await self._cancel_multipart_upload(exc)
        finally:
            await self._requests.cancel_all()
            io.RawIOBase.close(self)

    # The uploading underneath.

    def _raise_failed_upload(self) -> None:
        if (error := self._requests.error) is not None:
            raise OSError(f"Failed to upload a part of {self.path}") from error

    async def _upload_buffer(self, closing: bool) -> None:
        """Upload the parts the buffer fills, or all of it when ``closing``."""
        buffer = self._buffer
        cap = self.max_request_size or math.inf
        small = len(buffer) < self.min_multipart_size
        if not self._multipart and small and not closing:
            return

        # Closed with nothing sent, a file that is small, or would go as one
        # part, goes in one request: a multipart upload would add its start
        # and completion. A file of several parts goes in parts, sent at once,
        # which a storage may take faster than one request. Beyond the cap,
        # even a small file goes in parts.
        one_part = len(buffer) - min(self._block_size(len(buffer)), cap) < (
            self.min_block_size
        )
        if (
            closing
            and not self._multipart
            and (small or len(buffer) <= self.min_block_size or one_part)
            and len(buffer) <= cap
        ):
            # Even an empty file is created.
            data = bytes(buffer)
            buffer.clear()
            await self._upload_all(data)
            return

        block = min(self._block_size(len(buffer) if closing else self._pos), cap)
        offset = self._pos - len(buffer)
        taken = 0

        try:
            while taken < len(buffer):
                remaining = len(buffer) - taken
                if remaining - block >= self.min_block_size:
                    size = block
                elif closing:
                    # The last part takes the rest, which would be too small
                    # alone. Beyond the cap, a part of it is split off first:
                    # the rest is then less than min_block_size, so within it.
                    size = min(remaining, cap)
                else:
                    break

                if not self._multipart:
                    try:
                        await self._start_multipart_upload()
                    except BaseException as error:
                        # It may have started a session on the storage first.
                        await self._cancel_multipart_upload(error)
                        raise
                    self._multipart = True
                elif self.sequential:
                    # Waiting holds no permit: files sharing the limit go on.
                    await asyncio.wait([self._requests.queue[-1].task])
                    self._raise_failed_upload()

                start = offset + taken
                # One copy, through a view: a slice of the bytearray would copy
                # it once more. The view goes before the buffer is resized.
                with memoryview(buffer) as view:
                    part = bytes(view[taken : taken + size])
                await self._requests.send(
                    start,
                    start + size,
                    self._upload_part,
                    part,
                    start,
                    start + size,
                    len(self._requests.queue),
                    closing and size == remaining,
                )
                taken += size
        finally:
            # Even if cancelled between parts: the parts sent are never sent again.
            del buffer[:taken]

# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The engine under the strategies that change a storage, such as ``_tree_rm``.

They find what to change, e.g. by walking a tree or listing a prefix, and start
their jobs, e.g. deleting a file, as each item found arrives. `_Jobs` runs both
at once, each request holding a permit of one concurrency limit, and yields
what each step did. A family says where the items come from, in its
``source``. A strategy says what an item queues, how a job runs, and what its
outcome is.

`_CopyJobs` is the base of every copy's and move's jobs, whatever the
family: it places each path as fsspec's ``copy`` does. A family's copy joins
it with the family's jobs, e.g. ``_CopyOrMove(_CopyJobs, _TreeJobs)``: the
family finds what to copy, `_CopyJobs` where it goes. The two define no
method in common, and their constructors take keywords and pass the rest on,
so either order works.

This module depends on `operation` and `paths`.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import deque
from contextlib import aclosing
from glob import has_magic
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeVar

from fsspec.utils import common_prefix

from fsspec_alt_strategies.profile import StorageProfile
from fsspec_alt_strategies.states import CpState

from .operation import Operation, _collect_with_errors
from .paths import _trim

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        Awaitable,
        Callable,
        Sequence,
    )
    from typing import Self, TypeAlias

    _Transfer: TypeAlias = Callable[[dict[str, Any], str], Awaitable[None]]
    """How one file goes from its source entry to a destination path.

    It is the source's ``_cp_file_entry`` or ``_mv_file_entry`` within one
    storage, or a router's copy or move of a file between two. The entry is the
    source's, as its listing or ``_info`` gave it, its ``"name"`` the source
    path.
    """

_T = TypeVar("_T")


def _transfer_of(
    src_fs: Any,
    dst_fs: Any,
    transfer: _Transfer | None,
    same_storage: bool | None,
    name: str,
) -> tuple[_Transfer, bool]:
    """How a copy or move sends each file, and whether it stays in one storage.

    ``same_storage`` is whether ``dst_fs`` is ``src_fs``, unless given. Within
    one storage, a file goes by the source's own ``_cp_file_entry`` or
    ``_mv_file_entry``, as ``name``, "cp_entry" or "mv_entry", says, unless
    ``transfer`` is given. Between two, ``transfer`` is required: the source's
    own would write to the source.
    """
    if same_storage is None:
        same_storage = src_fs is dst_fs
    if transfer is None:
        if not same_storage:
            raise ValueError(f"{name} is required between two storages")
        transfer = getattr(src_fs, f"_{name.removesuffix('_entry')}_file_entry")
    return transfer, same_storage


class _Job(NamedTuple):
    """A request to make: its kind, e.g. "copy", its source, and where it goes.

    ``source`` is the source's entry, as its listing gave it, so that a copy or
    a move hands it on: at least its ``"name"``, all a path known otherwise
    has. ``paths`` are those of a request about several, e.g. a batch deletion.
    """

    kind: str
    source: dict[str, Any]
    destination: str = ""
    paths: tuple[str, ...] = ()

    @property
    def path(self) -> str:
        """The source's path."""
        return self.source["name"]  # type: ignore[no-any-return]


class _Roots(NamedTuple):
    """Where an operation starts: what it walks, and what it handles alone.

    Each root is its entry, so that a copy or move hands it on without asking
    it again. ``directories`` are walked, or listed, at once. A single path of
    unknown kind is among them, its entry its ``"name"`` alone, as its listing
    tells what it is. ``files`` are known files, e.g. glob matches.
    """

    directories: Sequence[dict[str, Any]]
    files: Sequence[dict[str, Any]] = ()

    @property
    def paths(self) -> list[str]:
        """The paths of all the roots, the directories first."""
        return [e["name"] for e in (*self.directories, *self.files)]


def _roots_of(
    matches: list[dict[str, Any]],
    errors: list[Exception],
    pattern: str | Sequence[str],
) -> _Roots:
    """The roots of a glob's matches, as fsspec's ``_expand_path`` finds them.

    A match inside a directory that matches is left out: walking the directory
    finds it. Matching nothing raises FileNotFoundError, as fsspec does.
    """
    if not matches and not errors:
        raise FileNotFoundError(pattern)

    directories: dict[str, dict[str, Any]] = {}
    files: list[dict[str, Any]] = []

    # Sorted, a directory comes before all in it.
    for entry in sorted(matches, key=lambda e: e["name"]):
        name = entry["name"].rstrip("/")
        parts = name.split("/")
        if any("/".join(parts[:i]) in directories for i in range(1, len(parts))):
            continue
        if entry["type"] == "directory":
            directories[name] = {**entry, "name": name}
        else:
            files.append({**entry, "name": name})

    return _Roots(list(directories.values()), files)


class _Queue(Generic[_T]):
    """Items waiting for a permit of a concurrency limit, and the requests running.

    Each item's request starts once a permit is free, and gives it back once
    done. When none is free, a waiter task gets the next one: awaited, in
    `pending`, along with the owner's requests, it picks up the permits that
    others sharing the limit give back. The owner takes each finished
    request's item back with `pop`.
    """

    def __init__(self, limit: asyncio.Semaphore) -> None:
        self.limit = limit
        self.waiting: deque[_T] = deque()
        self.running: dict[asyncio.Task[Any], _T] = {}
        self.waiter: asyncio.Task[bool] | None = None

    async def start(self, run: Callable[[_T], Awaitable[Any]]) -> None:
        """Start the request of each item waiting, while permits are free."""
        while self.waiting and await self._take():
            item = self.waiting.popleft()
            task = asyncio.ensure_future(run(item))
            task.add_done_callback(lambda _: self.limit.release())
            self.running[task] = item

    async def _take(self) -> bool:
        """Take a permit if one is free now, or got by the waiter."""
        if self.waiter is not None:
            if not self.waiter.done():
                return False
            self.waiter = None  # Its permit is now this queue's.
            return True

        if self.limit.locked():
            return False
        await self.limit.acquire()  # A permit is free: this does not wait.
        return True

    def pending(self) -> list[asyncio.Task[Any]]:
        """The requests running, and the waiter for a permit if items wait."""
        if self.waiting and self.waiter is None:
            self.waiter = asyncio.ensure_future(self.limit.acquire())
        return [*self.running, *([self.waiter] if self.waiting else [])]

    def pop(self, task: asyncio.Task[Any]) -> _T | None:
        """The item of a request done, or None for the waiter, whose permit `start` takes."""
        return self.running.pop(task, None)

    async def close(self) -> None:
        """Cancel the requests running, stop waiting, and give back a permit got."""
        tasks = list(self.running)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.running.clear()

        waiter, self.waiter = self.waiter, None
        if waiter is not None:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            if not waiter.cancelled() and waiter.exception() is None:
                self.limit.release()


class _Jobs(ABC):
    """The jobs of one operation on ``fs``, run while what it changes is found.

    Each request holds a permit of the filesystem's ``operation_limit``. The
    constructor takes keywords only, and passes on what it does not take, so
    that a subclass may join a family's jobs with another base, such as a
    copy's.
    """

    def __init__(
        self,
        *,
        fs: Any,
        op: Operation[Any, Any],
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fs = fs
        self.maxdepth = maxdepth

        # The directory roots a pattern matched: fsspec walks them one level less
        # deep than a path named, as its glob counted that level already.
        self.matched: set[str] = set()

        # The source's requests, the jobs, and a pattern's glob share it.
        self.concurrency_limit: asyncio.Semaphore = fs.operation_limit
        self.jobs: _Queue[_Job] = _Queue(self.concurrency_limit)

        # The queue's own, which the subclasses queue their jobs in.
        self.waiting = self.jobs.waiting
        self.running = self.jobs.running

        self.op = op
        self.errors = 0
        self.current: str | None = None

        # The errors found in finding the roots, e.g. by a glob: yielded first.
        self.found: list[Exception] = []

        # The source's next item, awaited along with the jobs.
        self.next: asyncio.Task[Any] | None = None

    @classmethod
    def operation(cls, **kwargs: Any) -> Operation[Any, Any]:
        """An operation running new jobs of this class, made at once with ``kwargs``.

        Made at once, the jobs give the operation its first state, e.g. whether
        a move renames directories, as they read it from the profile once.
        """
        jobs: Self | None = None
        op: Operation[Any, Any] = Operation(
            lambda _: jobs.outcomes(),  # type: ignore[union-attr]
            None,  # type: ignore[arg-type]  # The jobs' own, set below.
        )
        jobs = cls(op=op, **kwargs)
        op.state = jobs.state()
        return op

    # A hook, not abstract: only the jobs reading a profile await it.
    async def setup(self) -> None:  # noqa: B027
        """Read what the jobs need of the storage before preparing, e.g. its profile."""

    @abstractmethod
    async def prepare(self) -> _Roots | None:
        """What to walk, or None if there is nothing to do."""

    @abstractmethod
    def state(self) -> Any:
        """The operation's state now."""

    @abstractmethod
    def glob(self, pattern: str | Sequence[str]) -> Operation[Any, Any]:
        """The family's glob of ``pattern`` on ``fs``, within ``maxdepth``."""

    async def glob_roots(self, pattern: str | Sequence[str]) -> _Roots:
        """The roots matching glob patterns, noting those a pattern matched.

        The glob's errors go to ``found``, yielded first.
        """
        matches, errors = await _collect_with_errors(self.glob(pattern))
        roots = _roots_of(matches, errors, pattern)
        self.found.extend(errors)

        if self.maxdepth is not None:
            named = [pattern] if isinstance(pattern, str) else pattern
            literal = {_trim(p) for p in named if not has_magic(p)}
            self.matched = {
                d["name"] for d in roots.directories if d["name"] not in literal
            }
        return roots

    @abstractmethod
    def source(self, roots: _Roots) -> AsyncGenerator[Any] | None:
        """The items found in the root directories, if any: e.g. their listings."""

    @abstractmethod
    def fed(self, item: Any) -> list[Any]:
        """Queue the jobs for an item of the source, and return what to yield."""

    @abstractmethod
    def file_root(self, entry: dict[str, Any]) -> None:
        """Queue the jobs for a root of the operation which is a file, by its entry."""

    @abstractmethod
    def run(self, job: _Job) -> Awaitable[Any]:
        """The coroutine doing a job."""

    @abstractmethod
    def done(self, job: _Job) -> list[Any]:
        """Count a job done, queue what it allows, and return what to yield."""

    @abstractmethod
    def failed(self, job: _Job, error: Exception) -> list[Any]:
        """Handle a job's error, and return what to yield: the error, noted."""

    # A hook, not abstract: most strategies queue nothing at the source's end.
    def exhausted(self) -> None:  # noqa: B027
        """Queue what waits for the source's end, e.g. a last batch."""

    async def outcomes(self) -> AsyncGenerator[Any]:
        """Yield each outcome, as the operation does, keeping its state up to date."""
        await self.setup()
        self.op.state = self.state()
        roots = await self.prepare()

        try:
            # Yielded even when there is nothing to do: e.g. a list copied onto
            # itself but for a root whose name another one took.
            if self.found:
                self.errors += len(self.found)
                self.op.state = self.state()
                for error in self.found:
                    yield error

            if roots is None:
                return

            async with aclosing(self.steps(roots)) as steps:
                async for outcomes in steps:
                    self.op.state = self.state()
                    for outcome in outcomes:
                        yield outcome
        finally:
            # Closing the steps waits for the jobs running, and counts them.
            self.op.state = self.state()

    async def steps(self, roots: _Roots) -> AsyncGenerator[list[Any]]:
        """Run the jobs, yielding what each step did: an item, or a job done.

        Closing this stops the operation: it starts nothing new, and waits for
        the jobs running, as their outcome would otherwise be unknown.
        """
        source = self.source(roots)

        try:
            yield await self.begin(roots, source)

            while True:
                await self.start()
                if not (pending := self.pending()):
                    break

                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    if (outcome := self.take(task, source)) is not None:
                        yield outcome
        except GeneratorExit:
            self.waiting.clear()
            while self.running:
                done, _ = await asyncio.wait(self.running)
                for task in done:
                    self.finish(task)

            raise
        finally:
            # The source only reads, so cancelling it loses nothing. The jobs
            # are cancelled only when this task itself is.
            if self.next is not None:
                self.next.cancel()
                await asyncio.gather(self.next, return_exceptions=True)

            await self.jobs.close()
            if source is not None:
                await source.aclose()

    def pending(self) -> list[asyncio.Task[Any]]:
        """What to wait for: the jobs, the next item, and a permit if needed."""
        return [*self.jobs.pending(), *([self.next] if self.next else [])]

    async def begin(
        self, roots: _Roots, source: AsyncGenerator[Any] | None
    ) -> list[Any]:
        """Queue the files' jobs, and take the source's first item: the first step."""
        for entry in roots.files:
            self.file_root(entry)

        if source is None:
            self.exhausted()
            return []

        try:
            first = await anext(source)
        except StopAsyncIteration:
            self.exhausted()
            return []

        self.next = asyncio.ensure_future(anext(source))
        return self.fed(first)

    def take(
        self, task: asyncio.Task[Any], source: AsyncGenerator[Any] | None
    ) -> list[Any] | None:
        """Handle a finished task, returning its step, or None if it has none."""
        if task in self.running:
            return self.finish(task)
        if task is not self.next or source is None:
            return None  # The waiter's permit, which start takes.

        self.next = None
        try:
            item = task.result()
        except StopAsyncIteration:
            self.exhausted()
            return None

        self.next = asyncio.ensure_future(anext(source))
        return self.fed(item)

    def stopping(self) -> None:
        """Drop the jobs waiting, as the operation is cancelled.

        A subclass may keep those finishing what was done, e.g. the deletions
        of files a move has copied.
        """
        self.waiting.clear()

    async def start(self) -> None:
        """Start the jobs waiting, while permits are free, but none once cancelled."""
        if self.op.cancelling():
            self.stopping()
        await self.jobs.start(self.run)

    def finish(self, task: asyncio.Task[Any]) -> list[Any]:
        """Handle a job's task done, counting its errors, and return what to yield."""
        job = self.running.pop(task)
        self.current = job.path or job.destination

        try:
            task.result()
        except Exception as e:
            outcome = self.failed(job, e)
            self.errors += sum(isinstance(o, Exception) for o in outcome)
            return outcome

        return self.done(job)


class _CopyJobs(_Jobs):
    """The jobs of a copy or a move: where each path goes, onto ``dst_fs``.

    A path under ``prefix`` goes to the same place under ``base``. For a list,
    a path under a root of ``places`` goes there instead. `prepare` sets them,
    before `target` is used.

    ``dst_fs`` is the filesystem copied to, described by its profile, unknown
    if it has none. Within one storage, it is the source, and ``same_storage``
    is True: a path copied onto itself, or under itself, is refused. Between two, a path may go to the same path, or under
    it, as that is another storage's.
    """

    verb = "copy"
    doing = "Copying"

    def __init__(
        self,
        *,
        path1: str | Sequence[str],
        path2: str,
        dst_fs: Any,
        same_storage: bool,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.path1 = path1
        self.path2 = path2
        self.same_storage = same_storage

        # The profile is the destination's, as what a copy saves depends on
        # where it writes. Within one storage, it is the source's too. Unknown
        # until setup awaits it.
        self.dst_fs = dst_fs
        self.profile = StorageProfile()
        self.implicit_directories = False
        self.parents_required = True

        self.prefix = ""
        self.base = ""

        # For a list: where each root goes, by the root.
        self.places: dict[str, str] = {}

        # The destinations found missing, which need no asking again, e.g.
        # by a move renaming a root onto one.
        self.missing: set[str] = set()

        self.files_found = 0
        self.files_done = 0

    async def setup(self) -> None:
        # Any async filesystem may be a router's destination: fsspec gives it
        # no profile, so its storage is unknown.
        get = getattr(self.dst_fs, "_get_storage_profile", None)
        self.profile = await get() if get is not None else StorageProfile()
        # Unknown, directories are made first. Implicit directories need none.
        self.implicit_directories = bool(self.profile.implicit_directories)
        self.parents_required = (
            self.profile.parents_required is not False and not self.implicit_directories
        )

    def state(self) -> Any:
        return CpState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_copied=self.files_done,
        )

    async def prepare(self) -> _Roots | None:
        """Set where the paths go, as fsspec's ``copy`` places them."""
        destination = _trim(self.path2)

        if not isinstance(self.path1, str):
            roots = await self._place(destination)
            sources = roots.paths
        elif has_magic(self.path1):
            roots = await self.glob_roots(self.path1)
            sources = roots.paths
            if not sources:
                return roots  # Only errors, in found, to yield.
            self.prefix = common_prefix(sources)
            if not roots.directories and len(sources) == 1:
                # A single file goes into the destination.
                self.prefix = self.prefix.rpartition("/")[0]
            self.base = destination
        else:
            root = _trim(self.path1)
            if root == destination and self.same_storage:
                return None
            roots = _Roots([{"name": root}])
            sources = [root]
            self.prefix = root
            self.base = await self._base(root, destination)

        for source in sources if self.same_storage else ():
            target = self.target(source)
            if target == source and len(sources) == 1:
                return None
            if not source or target == source or target.startswith(source + "/"):
                raise ValueError(
                    f"Cannot {self.verb} {source!r} into itself: {target!r}"
                )

        return roots

    async def _place(self, destination: str) -> _Roots:
        """Place each root of a list into ``destination``, under its name.

        Two roots of the same name would go to the same place. As GNU's cp
        does, the first, in path order, goes, and each other is an error. That
        error is the root's own: what is in it is not copied either.
        """
        roots = await self.glob_roots(self.path1)
        directories = {d["name"]: d for d in roots.directories}
        files = {f["name"]: f for f in roots.files}
        placed: dict[str, str] = {}

        for source in sorted(roots.paths):
            name = source.rpartition("/")[2]
            target = f"{destination}/{name}" if destination else name
            if (other := placed.get(target)) is not None:
                error = FileExistsError(target)
                error.add_note(
                    f"{self.doing} {source!r} to {target!r} skipped: {other!r} goes"
                    " there too."
                )
                self.found.append(error)
                continue

            placed[target] = source
            self.places[source] = target

        return _Roots(
            [directories[s] for s in self.places if s in directories],
            [files[s] for s in self.places if s in files],
        )

    async def _base(self, root: str, destination: str) -> str:
        """Where ``root``, a single path, goes: into ``destination``, or onto it."""
        if self.path2.endswith("/"):
            into = True
        else:
            try:
                entry = await self.dst_fs._info(destination)
                into = entry["type"] == "directory"
            except FileNotFoundError:
                self.missing.add(destination)
                into = False

        if not into or self.path1.endswith("/"):
            return destination

        name = root.rsplit("/", 1)[-1]
        return f"{destination}/{name}" if destination else name

    def target(self, path: str) -> str:
        """Where a path goes."""
        if self.places:
            # The root it is under, found from the path up.
            root = path
            while root not in self.places and "/" in root:
                root = root.rpartition("/")[0]
            return self.places[root] + path[len(root) :]

        if self.prefix:
            return self.base + path[len(self.prefix) :]
        return f"{self.base}/{path}" if self.base else path

    async def copy_file(
        self, cp_entry: _Transfer, entry: dict[str, Any], destination: str
    ) -> None:
        """Copy a file by ``cp_entry``, replacing a file there the destination keeps.

        A destination refusing to overwrite, as fsspec's copy does not, costs
        an ``_info``, an ``_rm_file`` and a copy more. A directory there is no file
        to replace: ``_rm_file`` takes files only. The job holds its permit
        already. An error raised notes its paths, as it is yielded among paths.
        """
        source = entry["name"]
        try:
            try:
                await cp_entry(entry, destination)
            except FileExistsError:
                found = await self.dst_fs._info(destination)
                if found["type"] == "directory":
                    raise

                await self.dst_fs._rm_file(destination)
                await cp_entry(entry, destination)
        except Exception as e:
            e.add_note(f"{self.doing} {source!r} to {destination!r} failed.")
            raise

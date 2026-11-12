# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A concurrent walk of a directory tree, by recursive listings or a directory per listing.

`_tree_walk` yields each directory's ``(path, dirs, files)`` as its listing
arrives, as fsspec's ``walk`` gives them. A ``descend`` predicate prunes the
directories it lists, as glob needs.

Its engine, ``_chunks``, is the walk under the other tree strategies, such as
find, rm and du. It streams each directory's listing in parts, as ``_ils``
yields it, so that they act on a directory of hundreds of thousands of files as
it is listed. ``_tree_walk`` gathers the parts, as fsspec's ``walk`` gives a
directory whole.

A tree that nothing prunes is listed by one recursive ``_ils``, which gives it
depth first or in name order. Its entries are split into the same parts, each
directory's as soon as the listing has passed it. A storage with no recursive
listing of its own answers it with a directory per request, several at a time.
Where the walk prunes, it lists each directory it enters on its own, several at
a time, breadth first.

The tree strategies, and the engines under them, take the filesystem they
work on, and call its primitives, such as ``_ils``, under its
``operation_limit``.

This module depends on `jobs`, `operation` and `paths` of `common`.
"""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from fsspec_alt_strategies.states import WalkState

from ..common.jobs import _Queue
from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim, _triples

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Collection, Iterable

__all__: list[str] = []


def _tree_walk(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    topdown: bool = True,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    on_error: Literal["omit", "raise"] | Callable[[Exception], object] | None = None,
    ils_kwargs: dict[str, Any] | None = None,
) -> Operation[tuple[str, dict[str, Any], dict[str, Any]], WalkState]:
    """Walk a directory tree, listing several directories at a time.

    It yields each directory listed as ``(path, dirs, files)``, as fsspec's
    ``walk`` does with ``detail=True``: ``path`` itself first, then each
    directory below it. ``dirs`` and ``files`` map the names in the directory
    to their ``_ils`` entries. An empty directory yields empty ones. The
    directories come in the order their listings arrive, not in the tree's
    order. A directory that fails to list yields its error instead, and the
    walk goes on with the others. The error names its directory in its note,
    and as its ``directory`` attribute. ``on_error``, below, may handle those
    errors instead, as with fsspec's ``walk``.

    With ``topdown``, a directory comes before those below it, so each is
    listed on its own: a recursive listing passes a directory only after all
    below it, and the whole tree would be held. Without, a directory comes
    after all of those below it, as fsspec's ``walk`` orders them, and the tree
    is listed by one recursive ``_ils``, unless ``maxdepth`` or ``descend``
    prunes it. A failure then ends the walk of the whole tree, yielding its
    error as ``path``'s. A walk bottom up that is cancelled leaves out the
    directories whose subtrees were not done.

    Once cancelled, it lists no new directory, and yields the directories of the
    listings in flight. Iterating it raises these errors, which end it:

    - FileNotFoundError, if ``path`` does not exist;
    - NotADirectoryError, if ``path`` is a file;
    - with ``on_error="raise"``, the error of the first directory that failed to
      list.

    Requests: top down, or when pruned, one ``_ils`` per directory walked, as
    the filesystem's ``operation_limit`` allows. Bottom up otherwise, one
    recursive ``_ils`` of ``path``, which a storage without a recursive listing
    answers with one per directory.

    ``path`` is the directory the walk starts from, listed by the ``_ils`` of
    ``fs``. ``maxdepth`` of 1 lists only ``path``. ``topdown`` puts a directory
    before those below it. ``descend``, called with the entry of each
    subdirectory found, lets the walk list it only if it returns True.
    ``ils_kwargs`` go to each ``_ils``, as fsspec's ``walk`` passes its own on.

    ``on_error`` says what becomes of the error of a directory that fails to
    list:

    - None yields it, as every strategy does;
    - "omit" leaves it out;
    - "raise" raises it, ending the walk;
    - a callable is called with it, and it is left out.

    The operation yields ``(path, dirs, files)`` for each directory listed, or an
    error, and its state is a `WalkState`. The call raises ValueError if
    ``maxdepth`` is less than 1, or ``on_error`` is none of the above.
    """
    _check_maxdepth(maxdepth)
    if not (on_error in (None, "omit", "raise") or callable(on_error)):
        raise ValueError(f"on_error must be 'omit', 'raise' or callable: {on_error!r}")

    # Top down, a directory on its own: a descend makes the walk list so.
    walked = descend if descend is not None or not topdown else _every

    async def run(
        op: Operation[tuple[str, dict[str, Any], dict[str, Any]], WalkState],
    ) -> AsyncGenerator[tuple[str, dict[str, Any], dict[str, Any]] | Exception]:
        # Each directory's parts, held until its last, as fsspec's walk gives a
        # directory whole. Bottom up, a directory then waits for those below it.
        held: dict[str, list[dict[str, Any]]] = {}
        bottom_up = None if topdown else _BottomUp()
        chunks = _chunks(
            fs,
            path,
            maxdepth=maxdepth,
            descend=walked,
            ils_kwargs=ils_kwargs,
            stopping=op.cancelling,
        )

        async with aclosing(chunks):
            async for part in chunks:
                if (listing := _whole(part, held)) is None:
                    continue
                if listing.error is not None and on_error == "raise":
                    raise listing.error

                ready = [listing]
                if bottom_up is not None:
                    subdirs = _walked_subdirs(listing, maxdepth, walked)
                    ready = bottom_up.add(listing, subdirs)

                for listing in ready:
                    op.state = _walk_state(op.state, listing)
                    if (item := _walked(listing, on_error)) is not None:
                        yield item

    return Operation(run, WalkState(directories_listed=0))


def _whole(part: _Listing, held: dict[str, list[dict[str, Any]]]) -> _Listing | None:
    """A directory's whole listing once its last part comes, its parts held till then, or None.

    A failed listing comes whole with its error and no entries.
    """
    held.setdefault(part.directory, []).extend(part.entries)
    if not part.last:
        return None
    entries = held.pop(part.directory)
    if part.error is not None:
        return _Listing(part.directory, part.depth, [], part.error)
    else:
        return _Listing(part.directory, part.depth, entries)


def _walked(
    listing: _Listing,
    on_error: Literal["omit", "raise"] | Callable[[Exception], object] | None,
) -> tuple[str, dict[str, Any], dict[str, Any]] | Exception | None:
    """What the walk yields of a whole listing: its directory, its error, or None, as ``on_error`` says."""
    if listing.error is None:
        return _triples(listing.directory, listing.entries, detail=True)
    if on_error is None:
        return listing.error
    if callable(on_error):
        on_error(listing.error)
    return None  # "omit", or handled by the callable: left out.


def _walk_state(state: WalkState, listing: _Listing) -> WalkState:
    """The walk's state once a directory's listing is out."""
    return WalkState(
        errors=state.errors + (listing.error is not None),
        current=listing.directory,
        directories_listed=state.directories_listed + 1,
        found=state.found + len(listing.entries),
    )


def _walked_subdirs(
    listing: _Listing,
    maxdepth: int | None,
    descend: Callable[[dict[str, Any]], bool] | None,
) -> list[str]:
    """The subdirectories of a whole listing that the walk lists too, each once."""
    if listing.error is not None or (
        maxdepth is not None and listing.depth >= maxdepth
    ):
        return []
    else:
        return list(dict.fromkeys(_subdirs(listing.entries, descend)))


def _subdirs(
    entries: list[dict[str, Any]],
    descend: Callable[[dict[str, Any]], bool] | None,
) -> list[str]:
    """The subdirectories among entries to list next."""
    return [
        e["name"].rstrip("/")
        for e in entries
        if e["type"] == "directory" and (descend is None or descend(e))
    ]


def _every(entry: dict[str, Any]) -> bool:
    """A ``descend`` entering every directory, which lists each on its own."""
    return True


@dataclass(frozen=True, slots=True)
class _Listing:
    """A part of a directory's listing, as ``_chunks`` yields it, or all of it.

    ``depth`` is 1 for the directory the walk started from, 2 for its subdirectories,
    and so on. ``entries`` are as ``_ils`` gives them, without the directory itself,
    and empty if the listing failed with ``error``.

    A directory's listing comes in parts, each of at most ``chunk_size`` entries,
    as ``_ils`` yields them: ``first`` on the first part, ``last`` on the last.
    So an empty directory is one part, both first and last, with no entries. A
    failed listing ends with its error, as its last part. `_tree_walk` holds
    each directory whole: one part, both first and last.

    ``depth`` 0 is no listing, but the walk's root itself, as `_find_listings`
    yields it: ``entries`` hold only the root's ``info`` entry, under ``directory``.
    """

    directory: str
    depth: int
    entries: list[dict[str, Any]]
    error: Exception | None = None
    first: bool = True
    last: bool = True


_CHUNK_SIZE = 1000
"""The most entries of one part of a listing: a directory's are never all held."""


async def _chunks(
    fs: Any,
    path: str | Collection[str],
    *,
    maxdepth: int | None = None,
    matched: Collection[str] = (),
    descend: Callable[[dict[str, Any]], bool] | None = None,
    whole: Callable[[dict[str, Any]], bool] | None = None,
    ils_kwargs: dict[str, Any] | None = None,
    stopping: Callable[[], object] | None = None,
    chunk_size: int = _CHUNK_SIZE,
) -> AsyncGenerator[_Listing]:
    """Walk a directory tree, yielding each directory's listing in parts, as they come.

    It is the walk under the strategies acting on what they find, such as rm
    and find: a directory of hundreds of thousands of files is streamed, a part
    at a time, never held whole. A subdirectory is listed as soon as its entry
    comes, while its parent's listing goes on.

    ``path`` may be several directories, e.g. a glob's matches, walked at once,
    each as a root. None may be inside another. A root's listing failing before
    its first part raises, as fsspec's walk does, since nothing has been done
    yet: FileNotFoundError, or NotADirectoryError for a file. Any other failed
    listing is yielded as its last part, with its error, and its subdirectories
    not listed yet are not walked.

    The roots in ``matched`` start one level down, at depth 2: a pattern's
    matches are walked one level less deep than ``maxdepth``, as fsspec's
    ``_expand_path`` walks them.

    The parts of a directory come in order, but those of several directories
    mix, as the listings run at once. Each listing, by ``fs._ils`` with
    ``ils_kwargs``, holds a permit of ``fs.operation_limit`` while it runs.

    Once ``stopping`` returns true, no new listing starts, and those in flight
    stop after their current part: a directory stopped so has no last part.

    A tree that nothing prunes is listed whole by one recursive ``_ils``, split
    into the same parts, as `_produce_tree` says. A failure in it then ends
    the listing of the whole tree. Without ``maxdepth`` and ``descend``, that
    is each root. With ``descend``, ``whole`` tells of a directory's entry
    whether ``descend`` holds for every directory below it, e.g. below a glob's
    ``**``: such a directory is listed so, and the walk prunes only above it.
    With ``maxdepth``, each directory is listed on its own, so that none deeper
    is.
    """
    _check_maxdepth(maxdepth)

    roots = {_trim(p) for p in ([path] if isinstance(path, str) else path)}
    trees = _Trees(maxdepth, descend, whole)
    # The directories to list, each with its depth.
    directories: _Queue[tuple[str, int]] = _Queue(fs.operation_limit)
    # The parts listed, a few at a time: a listing waits while they are full.
    parts: asyncio.Queue[_Listing] = asyncio.Queue(maxsize=16)
    # The subdirectories found so far in each directory being listed, each listed once.
    found: dict[str, set[str]] = {}

    def stopped() -> bool:
        return stopping is not None and bool(stopping())

    async def start() -> None:
        if stopped():
            directories.waiting.clear()
        await directories.start(
            lambda item: (
                _produce_tree(fs, ils_kwargs or {}, *item, parts, chunk_size, stopped)
                if item[0] in trees.roots
                else _produce(
                    fs,
                    ils_kwargs or {},
                    *item,
                    item[0] in roots,
                    parts,
                    chunk_size,
                    stopped,
                )
            )
        )

    trees.add(roots)
    directories.waiting.extend(
        (root, 2 if root in matched else 1) for root in sorted(roots)
    )
    await start()

    try:
        while directories.waiting or directories.running or not parts.empty():
            if (part := await _next_part(parts, directories)) is None:
                await start()  # A listing ended, or a permit came: start more.
                continue

            if part.error is not None:
                # Raised here, it stops the walk before another listing starts.
                if part.directory in roots and part.first:
                    raise part.error
                _note(part.error, part.directory)
            elif not trees.listed(part.directory) and (
                maxdepth is None or part.depth < maxdepth
            ):
                subdirs = _new_subdirs(part, descend, found)
                trees.add(s for s, _ in subdirs)
                directories.waiting.extend(subdirs)

            if part.last:
                found.pop(part.directory, None)

            # Start the next listings before yielding, so that they run while
            # the caller handles this part.
            await start()
            yield part
    finally:
        await directories.close()


async def _next_part(
    parts: asyncio.Queue[_Listing], directories: _Queue[tuple[str, int]]
) -> _Listing | None:
    """The next part listed, or None once a listing ends, or a permit comes, first."""
    getter = asyncio.ensure_future(parts.get())
    try:
        done, _ = await asyncio.wait(
            [getter, *directories.pending()], return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            if task is not getter:
                directories.pop(task)  # A listing's parts are all queued already.
        return getter.result() if getter.done() else None
    finally:
        # A part not got yet stays in the queue: cancelling a get takes none.
        if not getter.done():
            getter.cancel()


async def _produce(
    fs: Any,
    ils_kwargs: dict[str, Any],
    directory: str,
    depth: int,
    root: bool,
    parts: asyncio.Queue[_Listing],
    chunk_size: int,
    stopped: Callable[[], bool],
) -> None:
    """List a directory, putting its parts in ``parts`` as they come, its error last."""
    first = True
    chunk: list[dict[str, Any]] = []
    try:
        entries = fs._ils(directory, **ils_kwargs)
        try:
            async for entry in entries:
                if entry["name"].rstrip("/") == directory:
                    # A file lists itself, as fsspec's _ls of a file does. Some
                    # storages list a directory within itself too, e.g. as its
                    # marker object.
                    if entry["type"] != "directory" and root:
                        raise _not_a_directory(directory, entry)
                    continue

                chunk.append(entry)
                if len(chunk) >= chunk_size:
                    await parts.put(
                        _Listing(directory, depth, chunk, None, first, False)
                    )
                    first, chunk = False, []
                    if stopped():
                        return
        finally:
            if (aclose := getattr(entries, "aclose", None)) is not None:
                await aclose()
    except Exception as e:
        await parts.put(_Listing(directory, depth, [], e, first, True))
        return

    await parts.put(_Listing(directory, depth, chunk, None, first, True))


async def _produce_tree(
    fs: Any,
    ils_kwargs: dict[str, Any],
    root: str,
    root_depth: int,
    parts: asyncio.Queue[_Listing],
    chunk_size: int,
    stopped: Callable[[], bool],
) -> None:
    """List a tree by one recursive ``_ils``, putting each directory's parts as a walk would.

    The entries come depth first or in name order, all below a directory
    together, so a directory is done once the listing has passed it: `_Split`
    puts its last part then. A failed listing puts what it held, then its error
    as the root's last part: the directories below it that were not done get
    none, as when stopped. So the error is the root's first part only if the
    root itself failed to list, and is then raised as such.
    """
    tree = _Split(root, root_depth, chunk_size)
    try:
        entries = fs._ils(root, recursive=True, **ils_kwargs)
        try:
            async for entry in entries:
                for part in tree.add(entry):
                    await parts.put(part)
                if stopped():
                    return
        finally:
            if (aclose := getattr(entries, "aclose", None)) is not None:
                await aclose()
    except Exception as e:
        # What was listed first, so that only the root's own failure is a first part.
        for part in tree.flush():
            await parts.put(part)
        await parts.put(_Listing(root, root_depth, [], e, tree.first, True))
        return

    for part in tree.end():
        await parts.put(part)


class _Split:
    """A recursive listing's entries, depth first or in name order, split into each directory's parts.

    All below a directory come together, so the walk keeps only the directories
    it is in, from the root down: those ``open``. A name not below the deepest
    one closes it, for none below it comes later. A directory's entry may come
    apart from what is below it, as "a/b-x" comes between "a/b" and "a/b/c" in
    name order: it waits as ``pending`` in its parent until a path below it
    opens it, or the listing passes it, empty. Depth first, its siblings may
    come in any order: what is below it comes right after it, so any other
    name tells it is empty. A sibling sorting before it only keeps it pending
    longer, never wrongly.

    A directory not listed, as an object store lists only files, gets an entry
    in its parent from the paths below it. A directory's entry comes in a part
    before any part of its own, as the strategies count what is in a directory
    by its parent's listing: before a part is put, those of the directories
    above it are. Once ``chunk_size`` entries are held in all, they are put.
    """

    def __init__(self, root: str, root_depth: int, chunk_size: int) -> None:
        self.root = root
        self.chunk_size = chunk_size
        self.open = [_Open(root, root_depth)]
        self.count = 0  # The entries held.

    @property
    def first(self) -> bool:
        """Whether no part of the root has been put yet."""
        return self.open[0].first if self.open else False

    def add(self, entry: dict[str, Any]) -> list[_Listing]:
        """Hold an entry in its directory, and the parts it completes."""
        name = entry["name"].rstrip("/")
        if name == self.root:
            # A file lists itself. A directory may list its own marker.
            if entry["type"] != "directory":
                raise _not_a_directory(self.root, entry)
            return []
        if not _below(name, self.root):
            return []  # No storage should list it.

        parts = self.close(name)
        top = self.open[-1]
        for directory in _between(top.name, name):
            if directory not in top.pending:
                top.hold({"name": directory, "size": 0, "type": "directory"})
                self.count += 1
            top.pending.pop(directory, None)
            top = _Open(directory, top.depth + 1)
            self.open.append(top)

        if entry["type"] == "directory":
            if name in top.pending:
                return parts  # Listed twice, e.g. as its marker too.
            top.pending[name] = None
        top.hold(entry)
        self.count += 1

        if self.count >= self.chunk_size:
            parts += self.flush()
        return parts

    def close(self, name: str) -> list[_Listing]:
        """The last parts of the directories that ``name`` comes after."""
        parts = []
        while len(self.open) > 1 and not _below(name, self.open[-1].name):
            parts += self.pop()

        top = self.open[-1]
        for directory in list(top.pending):
            # Name order: those after it are not passed either.
            if _below(name, directory) or name < _prefix(directory):
                break
            parts += self.empty(directory)
        return parts

    def end(self) -> list[_Listing]:
        """The last parts of all still open, once the listing has ended."""
        parts = []
        while self.open:
            parts += self.pop()
        return parts

    def pop(self) -> list[_Listing]:
        """The last parts of the deepest directory open, and of those pending in it."""
        top = self.open[-1]
        parts = [p for d in list(top.pending) for p in self.empty(d)]
        self.open.pop()
        parts += self.flush()
        parts.append(top.part(last=True))
        return parts

    def empty(self, directory: str) -> list[_Listing]:
        """The one part of a directory pending in the deepest one open: nothing is below it."""
        top = self.open[-1]
        del top.pending[directory]
        return [*self.flush(), _Listing(directory, top.depth + 1, [])]

    def flush(self) -> list[_Listing]:
        """The parts of all held, parents first."""
        self.count = 0
        return [o.part(last=False) for o in self.open if o.entries]


class _Open:
    """A directory the recursive listing is in, with its entries held."""

    def __init__(self, name: str, depth: int) -> None:
        self.name = name
        self.depth = depth
        self.entries: list[dict[str, Any]] = []
        self.first = True
        # Its subdirectories listed, below which nothing has come yet, in listing order.
        self.pending: dict[str, None] = {}

    def hold(self, entry: dict[str, Any]) -> None:
        self.entries.append(entry)

    def part(self, *, last: bool) -> _Listing:
        part = _Listing(self.name, self.depth, self.entries, None, self.first, last)
        self.entries, self.first = [], False
        return part


def _prefix(directory: str) -> str:
    """What the paths below a directory start with."""
    return directory if not directory or directory.endswith("/") else f"{directory}/"


def _below(name: str, directory: str) -> bool:
    return name.startswith(_prefix(directory))


def _between(directory: str, name: str) -> list[str]:
    """The directories below ``directory`` that ``name`` is in, parents first."""
    prefix = _prefix(directory)
    parts = name[len(prefix) :].split("/")[:-1]
    return [prefix + "/".join(parts[: i + 1]) for i in range(len(parts))]


def _new_subdirs(
    part: _Listing,
    descend: Callable[[dict[str, Any]], bool] | None,
    found: dict[str, set[str]],
) -> list[tuple[str, int]]:
    """The subdirectories a part adds to list, with their depth, those found before left out.

    Each comes once, though a storage may list a directory twice, e.g. with and
    without its marker object, in two parts.
    """
    seen = found.setdefault(part.directory, set())
    new = [s for s in dict.fromkeys(_subdirs(part.entries, descend)) if s not in seen]
    seen.update(new)
    return [(s, part.depth + 1) for s in new]


class _Trees:
    """The directories a walk lists whole, by one recursive ``_ils`` each.

    A directory is so listed when the walk prunes nothing below it: it has no
    ``maxdepth``, and no ``descend``, or ``whole`` holds for it. The directories
    below are then in its listing, never listed on their own.
    """

    def __init__(
        self,
        maxdepth: int | None,
        descend: Callable[[dict[str, Any]], bool] | None,
        whole: Callable[[dict[str, Any]], bool] | None,
    ) -> None:
        self.maxdepth = maxdepth
        self.descend = descend
        self.whole = whole
        self.roots: set[str] = set()

    def add(self, directories: Iterable[str]) -> None:
        """Note those of the directories to list that are listed whole."""
        if self.maxdepth is None:
            self.roots.update(d for d in directories if self._whole(d))

    def _whole(self, directory: str) -> bool:
        if self.descend is None:
            return True
        else:
            return self.whole is not None and self.whole(
                {"name": directory, "type": "directory"}
            )

    def listed(self, directory: str) -> bool:
        """Whether the directory's subdirectories are in a recursive listing."""
        return any(
            r in (directory, "") or directory.startswith(r + "/") for r in self.roots
        )


def _not_a_directory(root: str, entry: dict[str, Any]) -> NotADirectoryError:
    """The error of a root that is a file, with its entry as the listing gave it.

    A copy or move of the root then hands the entry on, with no ``_info``.
    """
    error = NotADirectoryError(root)
    error.entry = {**entry, "name": root}  # type: ignore[attr-defined]
    return error


def _note(error: Exception, directory: str) -> None:
    """Note its directory on a listing's error, yielded among directories.

    In its note for people, and as an attribute for code, e.g. fsspec's walk.
    """
    error.add_note(f"Listing {directory!r} failed.")
    error.directory = directory  # type: ignore[attr-defined]


class _BottomUp:
    """Holds each listing back until the listings of its subdirectories are out.

    A subdirectory's listing may end before its parent's: a recursive listing
    passes a directory only after all below it, and a long listing streamed
    ends after the short ones it found. Such a subdirectory, out already,
    is not waited for.
    """

    def __init__(self) -> None:
        # Each listing held back, with how many of its subdirectories are not out.
        self.held: dict[str, tuple[_Listing, int]] = {}
        self.parents: dict[str, str] = {}
        # The directories out before their parent's listing ended.
        self.out: set[str] = set()

    def add(self, listing: _Listing, subdirs: list[str]) -> list[_Listing]:
        """The listings ready once this one is in, deepest first."""
        waiting = [s for s in subdirs if s not in self.out]
        self.out.difference_update(subdirs)
        if waiting:
            self.held[listing.directory] = listing, len(waiting)
            self.parents.update(dict.fromkeys(waiting, listing.directory))
            return []

        ready = [listing]
        directory = listing.directory

        # A subtree done may complete its parent's, and so on up.
        while (parent := self.parents.pop(directory, None)) is not None:
            held, remaining = self.held[parent]
            if remaining > 1:
                self.held[parent] = held, remaining - 1
                return ready
            del self.held[parent]
            ready.append(held)
            directory = parent

        self.out.add(directory)  # Its parent's listing has not ended yet.
        return ready

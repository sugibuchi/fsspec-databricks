# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Base classes for a filesystem's file objects, by what its storage allows.

fsspec's buffered file reads through a cache whose type and block size the
developer picks, such as "readahead", "bytes" or "block". No one choice suits
both small random reads, such as a Parquet footer's, and bulk sequential reads.
And a file is opened long before its reads show which kind it gets.

- `adaptive` is for storages reading byte ranges and writing in parts. Its
  files adapt their requests to the reads and writes so far.
- `fsspec_transaction` writes a file in fsspec's transactions, in one of several
  strategies: a temporary path, a local buffer, or the storage's own.
- `fsspec_async` gives the files the API of those fsspec's ``open_async``
  returns, so that ``open`` and ``open_async`` share their requests.

All run their requests on an asyncio event loop, such as a filesystem's
``loop``, as their ``concurrency_limit``, an `asyncio.Semaphore`, allows.
Given a shared one, files share its limit: the caller chooses its scope, e.g.
a file, a filesystem, or the process. A filesystem subclasses them, implements
their requests, and returns them from ``_open``. The requests are the
storage's own: not fsspec's ``_cat_file`` or ``_put_file``, which many
filesystems implement with their file objects.

Each module holds its classes: import them from there, e.g.
``fsspec_alt_strategies.files.adaptive``. This package imports none of them.
"""

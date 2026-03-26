from io import BytesIO
from random import randbytes

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from aiohttp import (
    ClientHandlerType,
    ClientRequest,
    ClientResponse,
)

from fsspec_databricks.volume import VolumeReadableFile, VolumeWritableFile

from .utils import bytes_sig


async def _dummy_auth(
    request: ClientRequest, handler: ClientHandlerType
) -> ClientResponse:
    return await handler(request)


def test_volume_file_read(dummy_api, dummy_api_context, client_event_loop):
    data = randbytes(20 * 1000 * 1000)
    path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file"

    dummy_api_context.files[path] = data

    with VolumeReadableFile(
        path=path,
        host=dummy_api,
        auth=_dummy_auth,
        loop=client_event_loop,
        size=len(data),
    ) as f:
        actual = f.read()

    assert bytes_sig(data) == bytes_sig(actual)

    dummy_api_context.check_signed_url = True

    with pytest.raises(IOError):
        with VolumeReadableFile(
            path=path,
            host=dummy_api,
            auth=_dummy_auth,
            loop=client_event_loop,
            size=len(data),
            prefer_presigned_url=False,
        ) as f:
            _ = f.read()


def test_volume_file_read_parquet(
    client_event_loop,
    dummy_api,
    dummy_api_context,
    dummy_table,
    dummy_table_parquet,
):
    path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file.parquet"

    dummy_api_context.files[path] = dummy_table_parquet

    with VolumeReadableFile(
        path=path,
        host=dummy_api,
        auth=_dummy_auth,
        loop=client_event_loop,
        size=len(dummy_table_parquet),
    ) as f:
        df = pd.read_parquet(f)

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]


def test_volume_file_oneshot_write(
    dummy_api,
    dummy_api_context,
    client_event_loop,
):
    data = randbytes(1000 * 1000)
    path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file"

    with VolumeWritableFile(
        path=path,
        host=dummy_api,
        auth=_dummy_auth,
        loop=client_event_loop,
    ) as f:
        f.write(data)

    actual = dummy_api_context.files[path]
    assert bytes_sig(data) == bytes_sig(actual)


def test_volume_file_multipart_write(
    dummy_api,
    dummy_api_context,
    client_event_loop,
):
    data = randbytes(20 * 1000 * 1000)
    path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file"

    with VolumeWritableFile(
        path=path,
        host=dummy_api,
        auth=_dummy_auth,
        loop=client_event_loop,
    ) as f:
        f.write(data)

    actual = dummy_api_context.files[path]
    assert bytes_sig(data) == bytes_sig(actual)

    dummy_api_context.check_signed_url = True
    dummy_api_context.files.clear()

    with pytest.raises(IOError):
        with VolumeWritableFile(
            path=path,
            host=dummy_api,
            auth=_dummy_auth,
            loop=client_event_loop,
            use_presigned_url=False,
        ) as f:
            f.write(data)


def test_volume_file_resumable_write(
    dummy_api,
    dummy_api_context,
    client_event_loop,
):
    data = randbytes(20 * 1000 * 1000)
    path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file"

    dummy_api_context.upload_mode = "resumable"

    with VolumeWritableFile(
        path=path,
        host=dummy_api,
        auth=_dummy_auth,
        loop=client_event_loop,
    ) as f:
        f.write(data)

    actual = dummy_api_context.files[path]
    assert bytes_sig(data) == bytes_sig(actual)

    dummy_api_context.check_signed_url = True
    dummy_api_context.files.clear()

    with pytest.raises(IOError):
        with VolumeWritableFile(
            path=path,
            host=dummy_api,
            auth=_dummy_auth,
            loop=client_event_loop,
            use_presigned_url=False,
        ) as f:
            f.write(data)


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_abstract_async_readable_file_write_parquet(
    client_event_loop, dummy_api, dummy_api_context, dummy_table, upload_mode
):
    path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file.parquet"
    dummy_api_context.upload_mode = upload_mode

    with VolumeWritableFile(
        path=path,
        host=dummy_api,
        auth=_dummy_auth,
        loop=client_event_loop,
    ) as f:
        pq.write_table(pa.Table.from_pylist(dummy_table), f)

    df = pd.read_parquet(BytesIO(dummy_api_context.files[path]))

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]

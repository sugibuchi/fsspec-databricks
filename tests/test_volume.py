import asyncio
import socket
from io import BytesIO
from random import randbytes
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from aiohttp import (
    ClientHandlerType,
    ClientRequest,
    ClientResponse,
)

from fsspec_databricks.volume import (
    VolumeFileSystem,
    VolumeMultipartWritableFile,
    VolumeReadableFile,
    VolumeResumableWritableFile,
    _VolumeWritableFile,
)

from .utils import bytes_sig

path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file"


async def _dummy_auth(
    request: ClientRequest, handler: ClientHandlerType
) -> ClientResponse:
    return await handler(request)


def _reader(session, base_url, loop, data_size, **kwargs):
    return VolumeReadableFile(
        path,
        loop,
        data_size,
        session=session,
        base_url=base_url,
        auth=_dummy_auth,
        **kwargs,
    )


_writer_classes = {
    "multipart": VolumeMultipartWritableFile,
    "resumable": VolumeResumableWritableFile,
}


def _writer(session, base_url, loop, upload_mode="multipart", **kwargs):
    return _writer_classes[upload_mode](
        path,
        loop,
        session=session,
        base_url=base_url,
        auth=_dummy_auth,
        **kwargs,
    )


def test_volume_fs_storage_proxy_probe_timeout():
    client = MagicMock()
    client.config.authenticate.return_value = {}
    client.get_workspace_id.return_value = 1234

    # Connections are accepted by the backlog, but no response ever comes.
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]

        with VolumeFileSystem(client=client) as fs:
            fs._storage_proxy_hostname = f"http://127.0.0.1:{port}"
            fs._storage_proxy_probe_timeout = 0.1

            assert fs.storage_proxy_available is False


def test_volume_fs_storage_proxy_probe_connection_refused():
    client = MagicMock()
    client.config.authenticate.return_value = {}
    client.get_workspace_id.return_value = 1234

    # Nothing listens on the port once the server is closed.
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]

    with VolumeFileSystem(client=client) as fs:
        fs._storage_proxy_hostname = f"http://127.0.0.1:{port}"
        # Long enough for a refused connection on Windows, which retries for seconds.
        fs._storage_proxy_probe_timeout = 30.0

        assert fs.storage_proxy_available is False


def test_volume_file_read(
    dummy_api, dummy_api_context, client_event_loop, aiohttp_session
):
    data = randbytes(20 * 1000 * 1000)
    dummy_api_context.files[path] = data

    with _reader(aiohttp_session, dummy_api, client_event_loop, len(data)) as f:
        actual = f.read()

    assert bytes_sig(data) == bytes_sig(actual)

    dummy_api_context.check_signed_url = True

    with pytest.raises(IOError):
        with _reader(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            len(data),
            prefer_presigned_url=False,
        ) as f:
            _ = f.read()


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_volume_file_read_retries(
    dummy_api_context, dummy_api, client_event_loop, aiohttp_session, status
):
    data = randbytes(1000)
    dummy_api_context.files[path] = data
    dummy_api_context.get_file_errors.extend([status, status])

    with patch.object(VolumeReadableFile, "fetch_retry_delay", 0):
        with _reader(aiohttp_session, dummy_api, client_event_loop, len(data)) as f:
            assert f.read() == data

    assert not dummy_api_context.get_file_errors


def test_volume_file_read_retries_exhausted(
    dummy_api_context, dummy_api, client_event_loop, aiohttp_session
):
    data = randbytes(1000)
    dummy_api_context.files[path] = data
    dummy_api_context.get_file_errors.extend([500] * 10)

    with patch.object(VolumeReadableFile, "fetch_retry_delay", 0):
        with _reader(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            len(data),
            concurrency_limit=asyncio.Semaphore(1),
        ) as f:
            with pytest.raises(OSError):
                f.read()

    # The first request and its retries.
    attempts = 10 - len(dummy_api_context.get_file_errors)
    assert attempts == VolumeReadableFile.fetch_max_retries + 1


@pytest.mark.parametrize("status", [400, 403, 404])
def test_volume_file_read_no_retry(
    dummy_api_context, dummy_api, client_event_loop, aiohttp_session, status
):
    data = randbytes(1000)
    dummy_api_context.files[path] = data
    dummy_api_context.get_file_errors.extend([status] * 2)

    with patch.object(VolumeReadableFile, "fetch_retry_delay", 0):
        with _reader(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            len(data),
            concurrency_limit=asyncio.Semaphore(1),
        ) as f:
            with pytest.raises(OSError):
                f.read()

    assert len(dummy_api_context.get_file_errors) == 1


def test_volume_file_read_parquet(
    client_event_loop,
    dummy_api,
    dummy_api_context,
    dummy_table,
    dummy_table_parquet,
    aiohttp_session,
):
    dummy_api_context.files[path] = dummy_table_parquet

    with _reader(
        aiohttp_session, dummy_api, client_event_loop, len(dummy_table_parquet)
    ) as f:
        df = pd.read_parquet(f)

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]


def test_volume_file_oneshot_write(
    dummy_api, dummy_api_context, client_event_loop, aiohttp_session
):
    data = randbytes(1000 * 1000)

    with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
        f.write(data)

    assert bytes_sig(data) == bytes_sig(dummy_api_context.files[path])


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_volume_file_parts_write(
    dummy_api, dummy_api_context, client_event_loop, aiohttp_session, upload_mode
):
    data = randbytes(20 * 1000 * 1000)
    dummy_api_context.upload_mode = upload_mode

    with _writer(aiohttp_session, dummy_api, client_event_loop, upload_mode) as f:
        # Several writes, so that the parts grow
        for i in range(0, len(data), 1000 * 1000):
            f.write(data[i : i + 1000 * 1000])

    assert bytes_sig(data) == bytes_sig(dummy_api_context.files[path])

    dummy_api_context.check_signed_url = True
    dummy_api_context.files.clear()

    with pytest.raises(IOError):
        with _writer(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            upload_mode,
            use_presigned_url=False,
        ) as f:
            f.write(data)


@pytest.mark.parametrize("name", ["min_block_size", "max_block_size"])
def test_volume_file_part_granularity(
    dummy_api, client_event_loop, aiohttp_session, name
):
    with pytest.raises(ValueError, match="multiple of"):
        _writer(
            aiohttp_session, dummy_api, client_event_loop, **{name: 10 * 1024 * 1023}
        )


@pytest.mark.parametrize(
    "file_class",
    [VolumeReadableFile, VolumeMultipartWritableFile, VolumeResumableWritableFile],
)
def test_volume_file_invalid_spread(
    dummy_api, client_event_loop, aiohttp_session, file_class
):
    args = (1,) if file_class is VolumeReadableFile else ()

    with pytest.raises(ValueError):
        file_class(
            path,
            client_event_loop,
            *args,
            session=aiohttp_session,
            base_url=dummy_api,
            auth=_dummy_auth,
            spread=0,
        )


@pytest.mark.parametrize("abort_supported", [True, False])
def test_volume_file_multipart_write_abort(
    dummy_api,
    dummy_api_context,
    client_event_loop,
    aiohttp_session,
    abort_supported,
):
    dummy_api_context.abort_supported = abort_supported

    with patch.object(
        _VolumeWritableFile,
        "_upload_part",
        AsyncMock(side_effect=RuntimeError("Dummy")),
    ):
        with pytest.raises(OSError) as exc_info:
            with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
                f.write(randbytes(20 * 1000 * 1000))

    # The upload's failure is raised, not the abort's
    assert "Failed to upload a part" in str(exc_info.value)
    assert path not in dummy_api_context.files

    if abort_supported:
        assert dummy_api_context.multipart_upload_sessions == {}


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_volume_file_write_parquet(
    client_event_loop,
    dummy_api,
    dummy_api_context,
    dummy_table,
    upload_mode,
    aiohttp_session,
):
    dummy_api_context.upload_mode = upload_mode

    with _writer(aiohttp_session, dummy_api, client_event_loop, upload_mode) as f:
        pq.write_table(pa.Table.from_pylist(dummy_table), f)

    df = pd.read_parquet(BytesIO(dummy_api_context.files[path]))

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_volume_file_parts_write_abort_per_mode(
    dummy_api, dummy_api_context, client_event_loop, aiohttp_session, upload_mode
):
    dummy_api_context.upload_mode = upload_mode

    with patch.object(
        _writer_classes[upload_mode],
        "_send_part",
        AsyncMock(side_effect=RuntimeError("Dummy")),
    ):
        with pytest.raises(OSError, match="Failed to upload a part"):
            with _writer(
                aiohttp_session, dummy_api, client_event_loop, upload_mode
            ) as f:
                f.write(randbytes(20 * 1000 * 1000))

    assert path not in dummy_api_context.files
    assert dummy_api_context.multipart_upload_sessions == {}
    assert dummy_api_context.resumable_upload_sessions == {}


@pytest.mark.parametrize("use_presigned_url", [True, False])
@pytest.mark.parametrize(
    ("upload_mode", "server_mode"),
    [("multipart", "resumable"), ("resumable", "multipart")],
)
def test_volume_file_upload_mode_mismatch(
    dummy_api,
    dummy_api_context,
    client_event_loop,
    aiohttp_session,
    upload_mode,
    server_mode,
    use_presigned_url,
):
    dummy_api_context.upload_mode = server_mode

    with pytest.raises(OSError, match="upload_mode") as exc_info:
        with _writer(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            upload_mode,
            use_presigned_url=use_presigned_url,
        ) as f:
            f.write(randbytes(20 * 1000 * 1000))

    assert upload_mode in str(exc_info.value)
    assert server_mode in str(exc_info.value)
    assert path not in dummy_api_context.files
    # The session the server started is aborted.
    assert dummy_api_context.multipart_upload_sessions == {}
    assert dummy_api_context.resumable_upload_sessions == {}

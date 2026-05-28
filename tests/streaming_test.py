# tests/streaming_service_test.py
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.streaming as m


@pytest.fixture(autouse=True)
def reset_stream_task():
    m._stream_task = None
    yield
    m._stream_task = None


def test_resolve_stream_db_interval_success():
    result = m._resolve_stream_db_interval("SUBSCRIPTION_INTERVAL_ONE_MINUTE")
    assert result == "1MIN"


def test_resolve_stream_db_interval_fallback(monkeypatch):
    monkeypatch.setattr(
        m,
        "SUBSCRIPTION_INTERVAL_TO_STR",
        {},
    )
    result = m._resolve_stream_db_interval("SUBSCRIPTION_INTERVAL_ONE_MINUTE")
    assert result == "SUBSCRIPTION_INTERVAL_ONE_MINUTE"


def test_resolve_stream_db_interval_invalid():
    with pytest.raises(ValueError, match="Неизвестный интервал стриминга"):
        m._resolve_stream_db_interval("BAD_INTERVAL")


@pytest.mark.asyncio
async def test_start_streaming_skips_when_no_instruments(monkeypatch):
    monkeypatch.setattr(
        m,
        "settings",
        SimpleNamespace(
            stream_instruments_list=[],
            stream_interval="SUBSCRIPTION_INTERVAL_ONE_MINUTE",
        ),
    )

    create_task_mock = AsyncMock()
    monkeypatch.setattr(m.asyncio, "create_task", create_task_mock)

    await m.start_streaming()

    assert m._stream_task is None
    create_task_mock.assert_not_called()


@pytest.mark.asyncio
async def test_start_streaming_creates_task(monkeypatch):
    monkeypatch.setattr(
        m,
        "settings",
        SimpleNamespace(
            stream_instruments_list=["FIGI1", "FIGI2"],
            stream_interval="SUBSCRIPTION_INTERVAL_ONE_MINUTE",
        ),
    )

    created = {}

    class DummyTask:
        def done(self):
            return False

    def fake_create_task(coro, name=None):
        created["coro"] = coro
        created["name"] = name
        return DummyTask()

    monkeypatch.setattr(m.asyncio, "create_task", fake_create_task)

    await m.start_streaming()

    assert m._stream_task is not None
    assert created["name"] == "market_data_stream"

    # чтобы pytest не ругался на незавершённую корутину
    created["coro"].close()


@pytest.mark.asyncio
async def test_stop_streaming_no_task():
    m._stream_task = None
    await m.stop_streaming()
    assert m._stream_task is None


@pytest.mark.asyncio
async def test_stop_streaming_cancels_task():
    class DummyTask:
        def __init__(self):
            self.cancel_called = False

        def done(self):
            return False

        def cancel(self):
            self.cancel_called = True

        def __await__(self):
            async def _inner():
                raise asyncio.CancelledError
            return _inner().__await__()

    task = DummyTask()
    m._stream_task = task

    await m.stop_streaming()

    assert task.cancel_called is True


@pytest.mark.asyncio
async def test_stream_with_reconnect_calls_handle_stream_candle(monkeypatch):
    handled = []

    async def fake_handle_stream_candle(candle, interval):
        handled.append((candle, interval))

    async def fake_run_market_data_stream(instruments, interval_str, on_candle_callback):
        await on_candle_callback("candle-1")
        raise asyncio.CancelledError()

    monkeypatch.setattr(m, "handle_stream_candle", fake_handle_stream_candle)
    monkeypatch.setattr(m, "run_market_data_stream", fake_run_market_data_stream)

    with pytest.raises(asyncio.CancelledError):
        await m._stream_with_reconnect(
            instruments=["FIGI1"],
            interval_str="SUBSCRIPTION_INTERVAL_ONE_MINUTE",
        )

    assert handled == [("candle-1", "1MIN")]


@pytest.mark.asyncio
async def test_stream_with_reconnect_retries_after_error(monkeypatch):
    calls = {"run": 0}
    sleep_calls = []

    async def fake_run_market_data_stream(instruments, interval_str, on_candle_callback):
        calls["run"] += 1
        if calls["run"] == 1:
            raise Exception("boom")
        raise asyncio.CancelledError()

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(m, "run_market_data_stream", fake_run_market_data_stream)
    monkeypatch.setattr(m.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(m, "handle_stream_candle", AsyncMock())

    with pytest.raises(asyncio.CancelledError):
        await m._stream_with_reconnect(
            instruments=["FIGI1"],
            interval_str="SUBSCRIPTION_INTERVAL_ONE_MINUTE",
        )

    assert calls["run"] == 2
    assert sleep_calls == [5]


@pytest.mark.asyncio
async def test_stream_with_reconnect_resets_backoff_after_normal_end(monkeypatch):
    calls = {"run": 0}
    sleep_calls = []

    async def fake_run_market_data_stream(instruments, interval_str, on_candle_callback):
        calls["run"] += 1
        if calls["run"] == 1:
            return
        raise asyncio.CancelledError()

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(m, "run_market_data_stream", fake_run_market_data_stream)
    monkeypatch.setattr(m.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(m, "handle_stream_candle", AsyncMock())

    with pytest.raises(asyncio.CancelledError):
        await m._stream_with_reconnect(
            instruments=["FIGI1"],
            interval_str="SUBSCRIPTION_INTERVAL_ONE_MINUTE",
        )

    assert calls["run"] == 2
    assert sleep_calls == [5]
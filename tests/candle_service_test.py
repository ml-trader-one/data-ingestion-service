# tests/candle_service_test.py
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.candle_service as m


@pytest.fixture
def sample_candle():
    return SimpleNamespace(
        time=datetime(2024, 1, 10, 12, 0, 0),
        open="open_q",
        high="high_q",
        low="low_q",
        close="close_q",
        volume=123,
        is_complete=True,
        figi="FIGI123",
        instrument_uid="UID123",
    )


def test_candle_to_dict(monkeypatch, sample_candle):
    values = {
        "open_q": 100.0,
        "high_q": 110.0,
        "low_q": 90.0,
        "close_q": 105.0,
    }

    monkeypatch.setattr(m, "quotation_to_float", lambda x: values[x])

    result = m.candle_to_dict(
        sample_candle,
        figi="FIGI123",
        instrument_uid="UID123",
        interval="1d",
        source="historical",
    )

    assert result["figi"] == "FIGI123"
    assert result["instrument_uid"] == "UID123"
    assert result["interval"] == "1d"
    assert result["time"] == sample_candle.time
    assert result["open"] == 100.0
    assert result["high"] == 110.0
    assert result["low"] == 90.0
    assert result["close"] == 105.0
    assert result["volume"] == 123
    assert result["is_complete"] is True
    assert result["source"] == "historical"


def test_candle_to_dict_default_is_complete(monkeypatch):
    candle = SimpleNamespace(
        time=datetime(2024, 1, 10, 12, 0, 0),
        open="o",
        high="h",
        low="l",
        close="c",
        volume=10,
    )

    monkeypatch.setattr(m, "quotation_to_float", lambda x: 1.0)

    result = m.candle_to_dict(
        candle,
        figi="FIGI1",
        instrument_uid="UID1",
        interval="1m",
        source="stream",
    )

    assert result["is_complete"] is True
    assert result["source"] == "stream"


@pytest.mark.asyncio
async def test_save_candle_to_db(monkeypatch):
    execute_mock = AsyncMock()
    commit_mock = AsyncMock()

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        execute = execute_mock
        commit = commit_mock

    monkeypatch.setattr(m, "AsyncSessionLocal", lambda: DummySession())

    candle_dict = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1d",
        "time": datetime(2024, 1, 1, 0, 0, 0),
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 123,
        "is_complete": True,
        "source": "historical",
    }

    await m.save_candle_to_db(candle_dict)

    execute_mock.assert_awaited_once()
    commit_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_historical_empty(monkeypatch):
    async def fake_fetch_historical_candles(figi, from_, to, interval):
        if False:
            yield None

    save_mock = AsyncMock()
    publish_mock = AsyncMock()

    monkeypatch.setattr(m, "fetch_historical_candles", fake_fetch_historical_candles)
    monkeypatch.setattr(m, "save_candle_to_db", save_mock)
    monkeypatch.setattr(m, "publish_candle", publish_mock)

    result = await m.load_historical(
        figi="FIGI1",
        instrument_uid="UID1",
        from_=datetime(2024, 1, 1),
        to=datetime(2024, 1, 2),
        interval="1d",
    )

    assert result == {
        "figi": "FIGI1",
        "instrument_uid": "UID1",
        "interval": "1d",
        "saved": 0,
        "skipped": 0,
    }
    save_mock.assert_not_awaited()
    publish_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_load_historical_success(monkeypatch):
    candle1 = SimpleNamespace(
        time=datetime(2024, 1, 1, 10, 0, 0),
        open="o1",
        high="h1",
        low="l1",
        close="c1",
        volume=100,
        is_complete=True,
    )
    candle2 = SimpleNamespace(
        time=datetime(2024, 1, 1, 11, 0, 0),
        open="o2",
        high="h2",
        low="l2",
        close="c2",
        volume=200,
        is_complete=True,
    )

    async def fake_fetch_historical_candles(figi, from_, to, interval):
        yield candle1
        yield candle2

    mapping = {
        "o1": 10.0, "h1": 11.0, "l1": 9.0, "c1": 10.5,
        "o2": 20.0, "h2": 21.0, "l2": 19.0, "c2": 20.5,
    }

    save_mock = AsyncMock()
    publish_mock = AsyncMock()

    monkeypatch.setattr(m, "fetch_historical_candles", fake_fetch_historical_candles)
    monkeypatch.setattr(m, "quotation_to_float", lambda x: mapping[x])
    monkeypatch.setattr(m, "save_candle_to_db", save_mock)
    monkeypatch.setattr(m, "publish_candle", publish_mock)

    result = await m.load_historical(
        figi="FIGI1",
        instrument_uid="UID1",
        from_=datetime(2024, 1, 1),
        to=datetime(2024, 1, 2),
        interval="1h",
    )

    assert result["saved"] == 2
    assert result["figi"] == "FIGI1"
    assert result["instrument_uid"] == "UID1"
    assert result["interval"] == "1h"
    assert save_mock.await_count == 2
    assert publish_mock.await_count == 2

    first_payload = publish_mock.await_args_list[0].args[0]
    assert first_payload["time"] == candle1.time.isoformat()
    assert first_payload["source"] == "historical"


@pytest.mark.asyncio
async def test_handle_stream_candle(monkeypatch, sample_candle):
    monkeypatch.setattr(m, "quotation_to_float", lambda x: {
        "open_q": 100.0,
        "high_q": 110.0,
        "low_q": 90.0,
        "close_q": 105.0,
    }[x])

    save_mock = AsyncMock()
    publish_mock = AsyncMock()

    monkeypatch.setattr(m, "save_candle_to_db", save_mock)
    monkeypatch.setattr(m, "publish_candle", publish_mock)

    await m.handle_stream_candle(sample_candle, interval="1m")

    save_mock.assert_awaited_once()
    publish_mock.assert_awaited_once()

    saved_payload = save_mock.await_args.args[0]
    assert saved_payload["figi"] == "FIGI123"
    assert saved_payload["instrument_uid"] == "UID123"
    assert saved_payload["source"] == "stream"

    published_payload = publish_mock.await_args.args[0]
    assert published_payload["time"] == sample_candle.time.isoformat()
    assert published_payload["source"] == "stream"


@pytest.mark.asyncio
async def test_handle_stream_candle_uses_figi_as_instrument_uid(monkeypatch):
    candle = SimpleNamespace(
        time=datetime(2024, 1, 10, 12, 0, 0),
        open="o",
        high="h",
        low="l",
        close="c",
        volume=50,
        figi="FIGI_NO_UID",
    )

    monkeypatch.setattr(m, "quotation_to_float", lambda x: 1.0)

    save_mock = AsyncMock()
    publish_mock = AsyncMock()

    monkeypatch.setattr(m, "save_candle_to_db", save_mock)
    monkeypatch.setattr(m, "publish_candle", publish_mock)

    await m.handle_stream_candle(candle, interval="5m")

    saved_payload = save_mock.await_args.args[0]
    assert saved_payload["instrument_uid"] == "FIGI_NO_UID"
    assert saved_payload["figi"] == "FIGI_NO_UID"
    assert saved_payload["interval"] == "5m"
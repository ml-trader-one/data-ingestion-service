# tests/invest_client_test.py
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.invest_client as m


def test_quotation_to_float():
    q = SimpleNamespace(units=10, nano=500_000_000)
    assert m.quotation_to_float(q) == 10.5


def test_fetch_historical_candles_invalid_interval():
    with pytest.raises(ValueError, match="Неизвестный интервал"):
        async def _run():
            result = []
            async for item in m.fetch_historical_candles(
                figi="FIGI1",
                from_=datetime(2024, 1, 1),
                to=datetime(2024, 1, 2),
                interval="BAD_INTERVAL",
            ):
                result.append(item)
        import asyncio
        asyncio.run(_run())


@pytest.mark.asyncio
async def test_fetch_historical_candles_success(monkeypatch):
    candle1 = SimpleNamespace(id=1)
    candle2 = SimpleNamespace(id=2)

    class DummyClientInstance:
        async def get_all_candles(self, figi, from_, to, interval):
            assert figi == "FIGI1"
            assert from_ == datetime(2024, 1, 1)
            assert to == datetime(2024, 1, 2)
            assert interval == m.CANDLE_INTERVAL_MAP["1MIN"]
            yield candle1
            yield candle2

    class DummyAsyncClient:
        def __init__(self, token):
            self.token = token

        async def __aenter__(self):
            return DummyClientInstance()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(m, "AsyncClient", DummyAsyncClient)
    monkeypatch.setattr(m, "settings", SimpleNamespace(invest_token="TOKEN"))

    result = []
    async for item in m.fetch_historical_candles(
        figi="FIGI1",
        from_=datetime(2024, 1, 1),
        to=datetime(2024, 1, 2),
        interval="1MIN",
    ):
        result.append(item)

    assert result == [candle1, candle2]


@pytest.mark.asyncio
async def test_run_market_data_stream_invalid_interval():
    callback = AsyncMock()

    with pytest.raises(ValueError, match="Неизвестный интервал стриминга"):
        await m.run_market_data_stream(
            instruments=["FIGI1"],
            interval_str="BAD_INTERVAL",
            on_candle_callback=callback,
        )


@pytest.mark.asyncio
async def test_run_market_data_stream_success(monkeypatch):
    callback = AsyncMock()

    subscribe_calls = []

    class DummyCandlesStream:
        def waiting_close(self):
            return self

        def subscribe(self, candle_instruments):
            subscribe_calls.append(candle_instruments)

    class DummyStream:
        def __init__(self):
            self.candles = DummyCandlesStream()
            self._items = [
                SimpleNamespace(candle=SimpleNamespace(figi="FIGI1", close=123)),
                SimpleNamespace(candle=None),
                SimpleNamespace(candle=SimpleNamespace(figi="FIGI2", close=456)),
            ]

        def __aiter__(self):
            async def generator():
                for item in self._items:
                    yield item
            return generator()

    class DummyClientInstance:
        def create_market_data_stream(self):
            return DummyStream()

    class DummyAsyncClient:
        def __init__(self, token):
            self.token = token

        async def __aenter__(self):
            return DummyClientInstance()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class DummyCandleInstrument:
        def __init__(self, figi, interval):
            self.figi = figi
            self.interval = interval

    monkeypatch.setattr(m, "AsyncClient", DummyAsyncClient)
    monkeypatch.setattr(m, "CandleInstrument", DummyCandleInstrument)
    monkeypatch.setattr(m, "settings", SimpleNamespace(invest_token="TOKEN"))

    await m.run_market_data_stream(
        instruments=["FIGI1", "FIGI2"],
        interval_str="SUBSCRIPTION_INTERVAL_ONE_MINUTE",
        on_candle_callback=callback,
    )

    assert len(subscribe_calls) == 1
    assert len(subscribe_calls[0]) == 2
    assert subscribe_calls[0][0].figi == "FIGI1"
    assert subscribe_calls[0][1].figi == "FIGI2"
    assert subscribe_calls[0][0].interval == m.SUBSCRIPTION_INTERVAL_MAP["SUBSCRIPTION_INTERVAL_ONE_MINUTE"]

    assert callback.await_count == 2
    first_candle = callback.await_args_list[0].args[0]
    second_candle = callback.await_args_list[1].args[0]
    assert first_candle.figi == "FIGI1"
    assert second_candle.figi == "FIGI2"
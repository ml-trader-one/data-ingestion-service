from datetime import datetime
from typing import AsyncGenerator, Callable, Awaitable

from t_tech.invest import (
    AsyncClient,
    CandleInstrument,
    CandleInterval,
    HistoricCandle,
    SubscriptionInterval,
)
from t_tech.invest.async_services import AsyncMarketDataStreamManager

from app.config import settings

CANDLE_INTERVAL_MAP: dict[str, CandleInterval] = {
    "1MIN": CandleInterval.CANDLE_INTERVAL_1_MIN,
    "5MIN": CandleInterval.CANDLE_INTERVAL_5_MIN,
    "15MIN": CandleInterval.CANDLE_INTERVAL_15_MIN,
    "30MIN": CandleInterval.CANDLE_INTERVAL_30_MIN,
    "1HOUR": CandleInterval.CANDLE_INTERVAL_HOUR,
    "2HOUR": CandleInterval.CANDLE_INTERVAL_2_HOUR,
    "4HOUR": CandleInterval.CANDLE_INTERVAL_4_HOUR,
    "1DAY": CandleInterval.CANDLE_INTERVAL_DAY,
    "1WEEK": CandleInterval.CANDLE_INTERVAL_WEEK,
    "1MONTH": CandleInterval.CANDLE_INTERVAL_MONTH,
}

SUBSCRIPTION_INTERVAL_MAP: dict[str, SubscriptionInterval] = {
    "SUBSCRIPTION_INTERVAL_ONE_MINUTE": SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
    "SUBSCRIPTION_INTERVAL_FIVE_MINUTES": SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES,
    "SUBSCRIPTION_INTERVAL_ONE_HOUR": SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_HOUR,
    "SUBSCRIPTION_INTERVAL_ONE_DAY": SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY,
}

SUBSCRIPTION_INTERVAL_TO_STR: dict[str, str] = {
    "SUBSCRIPTION_INTERVAL_ONE_MINUTE":   "1MIN",
    "SUBSCRIPTION_INTERVAL_FIVE_MINUTES": "5MIN",
    "SUBSCRIPTION_INTERVAL_ONE_HOUR":     "1HOUR",
    "SUBSCRIPTION_INTERVAL_ONE_DAY":      "1DAY",
}


def quotation_to_float(q) -> float:
    return q.units + q.nano / 1_000_000_000


async def fetch_historical_candles(
    figi: str,
    from_: datetime,
    to: datetime,
    interval: str,
) -> AsyncGenerator[HistoricCandle, None]:
    candle_interval = CANDLE_INTERVAL_MAP.get(interval.upper())
    if candle_interval is None:
        raise ValueError(
            f"Неизвестный интервал: {interval}. "
            f"Допустимые: {list(CANDLE_INTERVAL_MAP.keys())}"
        )

    async with AsyncClient(settings.invest_token) as client:
        async for candle in client.get_all_candles(
            figi=figi,
            from_=from_,
            to=to,
            interval=candle_interval,
        ):
            yield candle


async def run_market_data_stream(
    instruments: list[str],
    interval_str: str,
    on_candle_callback: Callable[[object], Awaitable[None]],
) -> None:
    sub_interval = SUBSCRIPTION_INTERVAL_MAP.get(interval_str)
    if sub_interval is None:
        raise ValueError(
            f"Неизвестный интервал стриминга: {interval_str}. "
            f"Допустимые: {list(SUBSCRIPTION_INTERVAL_MAP.keys())}"
        )

    candle_instruments = [
        CandleInstrument(figi=figi, interval=sub_interval)
        for figi in instruments
    ]

    async with AsyncClient(settings.invest_token) as client:
        stream: AsyncMarketDataStreamManager = client.create_market_data_stream()
        stream.candles.waiting_close().subscribe(candle_instruments)

        async for market_data in stream:
            if market_data.candle:
                await on_candle_callback(market_data.candle)
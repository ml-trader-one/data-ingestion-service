import asyncio

import structlog

from app.candle_service import handle_stream_candle
from app.config import settings
from app.invest_client import run_market_data_stream, SUBSCRIPTION_INTERVAL_TO_STR

logger = structlog.get_logger(__name__)

_stream_task: asyncio.Task | None = None


async def start_streaming() -> None:
    global _stream_task

    instruments = settings.stream_instruments_list
    interval = settings.stream_interval

    if not instruments:
        logger.warning("No stream instruments configured, streaming skipped")
        return

    logger.info(
        "Starting market data stream",
        instruments=instruments,
        interval=interval,
    )

    _stream_task = asyncio.create_task(
        _stream_with_reconnect(instruments, interval),
        name="market_data_stream",
    )


async def stop_streaming() -> None:
    global _stream_task
    if _stream_task and not _stream_task.done():
        _stream_task.cancel()
        try:
            await _stream_task
        except asyncio.CancelledError:
            pass
        logger.info("Market data stream stopped")


async def _stream_with_reconnect(instruments: list[str], interval_str: str) -> None:
    backoff = 5
    max_backoff = 60

    db_interval = SUBSCRIPTION_INTERVAL_TO_STR.get(interval_str, interval_str)

    async def on_candle(candle):
        await handle_stream_candle(candle, interval=db_interval)

    while True:
        try:
            await run_market_data_stream(
                instruments=instruments,
                interval_str=interval_str,
                on_candle_callback=on_candle,
            )
        except asyncio.CancelledError:
            logger.info("Stream task cancelled")
            raise
        except Exception as exc:
            logger.error(
                "Stream disconnected, reconnecting",
                error=str(exc),
                backoff_seconds=backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        else:
            logger.warning("Stream ended unexpectedly, restarting in 5s")
            await asyncio.sleep(5)
            backoff = 5
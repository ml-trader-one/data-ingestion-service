from datetime import datetime

from sqlalchemy.dialects.postgresql import insert
import structlog

from app.config import settings
from app.database import AsyncSessionLocal, Candle, OutboxEvent
from app.invest_client import fetch_historical_candles, quotation_to_float
from app.kafka_producer import flush_pending_outbox

logger = structlog.get_logger(__name__)


def candle_to_dict(
    candle,
    figi: str,
    instrument_uid: str,
    interval: str,
    source: str = "historical",
) -> dict:
    return {
        "instrument_uid": instrument_uid,
        "figi": figi,
        "interval": interval,
        "time": candle.time,
        "open": quotation_to_float(candle.open),
        "high": quotation_to_float(candle.high),
        "low": quotation_to_float(candle.low),
        "close": quotation_to_float(candle.close),
        "volume": candle.volume,
        # HistoricCandle имеет is_complete, стриминговый Candle — нет
        # waiting_close() гарантирует что свеча завершена, поэтому дефолт True
        "is_complete": getattr(candle, "is_complete", True),
        "source": source,
    }


def candle_to_kafka_payload(candle_dict: dict) -> dict:
    return {
        **candle_dict,
        "time": candle_dict["time"].isoformat(),
    }


async def save_candle_to_db(candle_dict: dict) -> None:
    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = (
                insert(Candle)
                .values(
                    instrument_uid=candle_dict["instrument_uid"],
                    figi=candle_dict["figi"],
                    interval=candle_dict["interval"],
                    time=candle_dict["time"],
                    open=candle_dict["open"],
                    high=candle_dict["high"],
                    low=candle_dict["low"],
                    close=candle_dict["close"],
                    volume=candle_dict["volume"],
                    is_complete=candle_dict.get("is_complete", True),
                    source=candle_dict.get("source", "historical"),
                )
                .on_conflict_do_update(
                    constraint="pk_candle",
                    set_={
                        "open": candle_dict["open"],
                        "high": candle_dict["high"],
                        "low": candle_dict["low"],
                        "close": candle_dict["close"],
                        "volume": candle_dict["volume"],
                        "is_complete": candle_dict.get("is_complete", True),
                        "source": candle_dict.get("source", "historical"),
                    },
                )
            )

            await session.execute(stmt)
            session.add(
                OutboxEvent(
                    topic=settings.kafka_topic_raw_candles,
                    message_key=candle_dict["instrument_uid"],
                    payload=candle_to_kafka_payload(candle_dict),
                )
            )


async def load_historical(
    figi: str,
    instrument_uid: str,
    from_: datetime,
    to: datetime,
    interval: str,
) -> dict:
    saved = 0
    log = logger.bind(
        figi=figi,
        instrument_uid=instrument_uid,
        interval=interval,
        from_=from_.isoformat(),
        to=to.isoformat(),
    )
    log.info("Starting historical load")

    async for candle in fetch_historical_candles(figi, from_, to, interval):
        candle_dict = candle_to_dict(
            candle,
            figi=figi,
            instrument_uid=instrument_uid,
            interval=interval,
            source="historical",
        )
        await save_candle_to_db(candle_dict)
        saved += 1

    try:
        published = await flush_pending_outbox()
    except Exception as exc:
        published = 0
        log.warning("Outbox flush failed after historical load", error=str(exc))

    log.info("Historical load outbox flush complete", published=published)
    log.info("Historical load complete", saved=saved)
    return {"figi": figi, "instrument_uid": instrument_uid, "interval": interval, "saved": saved, "skipped": 0}


async def handle_stream_candle(candle, interval: str) -> None:
    figi = candle.figi
    instrument_uid = getattr(candle, "instrument_uid", figi)

    candle_dict = candle_to_dict(
        candle,
        figi=figi,
        instrument_uid=instrument_uid,
        interval=interval,
        source="stream",
    )
    await save_candle_to_db(candle_dict)
    try:
        published = await flush_pending_outbox()
    except Exception as exc:
        published = 0
        logger.warning("Outbox flush failed after stream candle", error=str(exc))

    logger.info(
        "Stream candle saved",
        figi=figi,
        instrument_uid=instrument_uid,
        interval=interval,
        time=candle_dict["time"].isoformat(),
        published=published,
    )

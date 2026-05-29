import json
from datetime import datetime
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer
from sqlalchemy import select

from app.config import settings
from app.database import AsyncSessionLocal, OutboxEvent
from app.metrics import (
    KAFKA_MESSAGES_PRODUCED,
    KAFKA_PRODUCE_DURATION,
)

logger = structlog.get_logger(__name__)

_producer: AIOKafkaProducer | None = None


def _json_serializer(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=_json_serializer).encode(),
            compression_type="zstd",
        )
        await _producer.start()
        logger.info("Kafka producer started", servers=settings.kafka_bootstrap_servers)
    return _producer


async def stop_producer():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped")


async def publish_candle(candle_dict: dict[str, Any]) -> None:
    producer = await get_producer()
    topic = settings.kafka_topic_raw_candles
    with KAFKA_PRODUCE_DURATION.labels(topic=topic).time():
        try:
            await producer.send_and_wait(
                settings.kafka_topic_raw_candles,
                value=candle_dict,
                key=candle_dict["instrument_uid"].encode(),  # партиционирование по instrument_uid
            )
        except Exception:
            KAFKA_MESSAGES_PRODUCED.labels(topic=topic, status="error").inc()
        else:
            KAFKA_MESSAGES_PRODUCED.labels(topic=topic, status="success").inc()


async def flush_pending_outbox(limit: int = 100) -> int:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.status == "pending")
            .order_by(OutboxEvent.id.asc())
            .limit(limit)
        )
        events = result.scalars().all()

        if not events:
            return 0

        producer = await get_producer()
        published = 0

        for event in events:
            topic = settings.kafka_topic_raw_candles
            with KAFKA_PRODUCE_DURATION.labels(topic=topic).time():
                try:
                    await producer.send_and_wait(
                        event.topic,
                        value=event.payload,
                        key=event.message_key.encode(),
                    )
                except Exception as exc:
                    KAFKA_MESSAGES_PRODUCED.labels(topic=topic, status="error").inc()
                    event.attempts += 1
                    event.last_error = str(exc)
                    await session.commit()
                    logger.warning(
                        "Outbox publish failed",
                        event_id=event.id,
                        topic=event.topic,
                        error=str(exc),
                    )
                else:
                    KAFKA_MESSAGES_PRODUCED.labels(topic=topic, status="success").inc()
                    event.status = "published"
                    event.attempts += 1
                    event.last_error = None
                    event.published_at = datetime.utcnow()
                    await session.commit()
                    published += 1

        return published

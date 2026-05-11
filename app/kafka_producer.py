import json
from datetime import datetime
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer

from app.config import settings

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
    await producer.send_and_wait(
        settings.kafka_topic_raw_candles,
        value=candle_dict,
        key=candle_dict["instrument_uid"].encode(),  # партиционирование по instrument_uid
    )
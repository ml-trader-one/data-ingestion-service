from prometheus_client import Counter, Histogram

KAFKA_MESSAGES_PRODUCED = Counter(
    "kafka_messages_produced_total",
    "Количество отправленных сообщений",
    labelnames=["topic", "status"],
)

KAFKA_PRODUCE_DURATION = Histogram(
    "kafka_produce_duration_seconds",
    "Время отправки одного сообщения в Kafka",
    labelnames=["topic"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)
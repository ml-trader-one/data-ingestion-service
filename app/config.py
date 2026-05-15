from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    invest_token: str = Field(..., alias="INVEST_TOKEN")

    postgres_host: str = Field("localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")
    postgres_db: str = Field("market_data", alias="POSTGRES_DB")
    postgres_user: str = Field("postgres", alias="POSTGRES_USER")
    postgres_password: str = Field("postgres", alias="POSTGRES_PASSWORD")

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_raw_candles: str = Field("raw_candles", alias="KAFKA_TOPIC_RAW_CANDLES")

    stream_instruments: str = Field("BBG004730N88", alias="STREAM_INSTRUMENTS")
    stream_interval: str = Field(
        "SUBSCRIPTION_INTERVAL_ONE_DAY",
        alias="STREAM_INTERVAL",
    )

    app_host: str = Field("0.0.0.0", alias="APP_HOST")
    app_port: int = Field(8000, alias="APP_PORT")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    loki_url: str = Field("localhost:3100", alias="LOKI_URL")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def stream_instruments_list(self) -> List[str]:
        return [i.strip() for i in self.stream_instruments.split(",") if i.strip()]


settings = Settings()
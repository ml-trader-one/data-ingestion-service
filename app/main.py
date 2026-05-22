import logging
import logging_loki
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import settings
from app.database import create_tables
from app.kafka_producer import flush_pending_outbox, stop_producer
from app.router import router
from app.streaming import start_streaming, stop_streaming

SERVICE_LOGGER_NAME = "data_ingestion_service"


def configure_logging():
    root_logger = logging.getLogger()
    existing_service_handlers = [
        handler
        for handler in root_logger.handlers
        if getattr(handler, "_data_ingestion_service_handler", False)
    ]
    for handler in existing_service_handlers:
        root_logger.removeHandler(handler)
        handler.close()

    loki_handler = logging_loki.LokiHandler(
        url=f"{settings.loki_url}/loki/api/v1/push",
        tags={"service": SERVICE_LOGGER_NAME},
        version="1",
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer()
            if settings.log_level == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler._data_ingestion_service_handler = True

    loki_handler._data_ingestion_service_handler = True
    root_logger.addHandler(handler)
    root_logger.addHandler(loki_handler)
    root_logger.setLevel(logging.getLevelName(settings.log_level))

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    log = structlog.get_logger(__name__)

    log.info("Starting ingestion service...")
    await create_tables()
    log.info("Database tables ready")
    try:
        published = await flush_pending_outbox()
        log.info("Outbox flush complete", published=published)
    except Exception as exc:
        log.warning("Outbox flush failed on startup", error=str(exc))

    await start_streaming()

    yield

    log.info("Shutting down ingestion service...")
    await stop_streaming()
    await stop_producer()
    log.info("Ingestion service stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Ingestion Service",
        description=(
            "Сервис загрузки рыночных данных из T-Invest API. "
            "Поддерживает исторические загрузки по запросу и автоматический стриминг."
        ),
        version="0.1.0",
        lifespan=lifespan,
        root_path="/ingestion"
    )
    app.include_router(router)
    return app


app = create_app()
Instrumentator().instrument(app).expose(app)


def run():
    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()

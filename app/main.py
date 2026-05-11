import logging
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI

from app.config import settings
from app.database import create_tables
from app.kafka_producer import stop_producer
from app.router import router
from app.streaming import start_streaming, stop_streaming


def configure_logging():
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if settings.log_level == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    log = structlog.get_logger(__name__)

    log.info("Starting ingestion service...")
    await create_tables()
    log.info("Database tables ready")

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
    )
    app.include_router(router)
    return app


app = create_app()


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
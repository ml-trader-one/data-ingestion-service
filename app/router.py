from datetime import datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from app.candle_service import load_historical
from app.invest_client import CANDLE_INTERVAL_MAP

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["ingestion"])


class HistoricalLoadRequest(BaseModel):
    figi: str = Field(..., description="FIGI инструмента, например BBG004730N88")
    instrument_uid: str = Field(..., description="UID инструмента из T-Invest API")
    from_date: datetime = Field(..., description="Дата начала загрузки")
    to_date: datetime = Field(..., description="Дата окончания загрузки")
    interval: str = Field(
        "1DAY",
        description=f"Таймфрейм. Допустимые: {list(CANDLE_INTERVAL_MAP.keys())}",
    )


class HistoricalLoadResponse(BaseModel):
    figi: str
    interval: str
    saved: int
    skipped: int
    status: str = "completed"


class HistoricalLoadBackground(BaseModel):
    figi: str
    interval: str
    status: str = "started"
    message: str


def _validate_historical_request(request: HistoricalLoadRequest) -> None:
    if request.from_date >= request.to_date:
        raise HTTPException(status_code=422, detail="from_date должна быть раньше to_date")

    if request.interval.upper() not in CANDLE_INTERVAL_MAP:
        raise HTTPException(
            status_code=422,
            detail=f"Неизвестный интервал: {request.interval}. "
            f"Допустимые: {list(CANDLE_INTERVAL_MAP.keys())}",
        )


@router.post("/candles/load", response_model=HistoricalLoadResponse)
async def load_candles(request: HistoricalLoadRequest) -> HistoricalLoadResponse:
    _validate_historical_request(request)

    try:
        result = await load_historical(
            figi=request.figi,
            instrument_uid=request.instrument_uid,
            from_=request.from_date,
            to=request.to_date,
            interval=request.interval.upper(),
        )
        return HistoricalLoadResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error("Historical load failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")


@router.post("/candles/load-async", response_model=HistoricalLoadBackground, status_code=202)
async def load_candles_async(
    request: HistoricalLoadRequest,
    background_tasks: BackgroundTasks,
) -> HistoricalLoadBackground:
    _validate_historical_request(request)

    background_tasks.add_task(
        load_historical,
        figi=request.figi,
        instrument_uid=request.instrument_uid,
        from_=request.from_date,
        to=request.to_date,
        interval=request.interval.upper(),
    )

    return HistoricalLoadBackground(
        figi=request.figi,
        interval=request.interval.upper(),
        status="started",
        message="Загрузка запущена в фоне. Следите за логами сервиса.",
    )


@router.get("/candles")
async def get_candles(
    figi: Annotated[str, Query(description="FIGI инструмента")],
    from_date: Annotated[datetime, Query(description="Дата начала")],
    to_date: Annotated[datetime, Query(description="Дата окончания")],
    interval: Annotated[str, Query(description="Таймфрейм")] = "1DAY",
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
):
    from sqlalchemy import select
    from app.database import AsyncSessionLocal, Candle

    async with AsyncSessionLocal() as session:
        stmt = (
            select(Candle)
            .where(
                Candle.figi == figi,
                Candle.interval == interval.upper(),
                Candle.time >= from_date,
                Candle.time <= to_date,
            )
            .order_by(Candle.time.asc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        candles = result.scalars().all()

    return {
        "figi": figi,
        "interval": interval,
        "count": len(candles),
        "candles": [
            {
                "time": c.time,
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "volume": c.volume,
                "is_complete": c.is_complete,
                "source": c.source,
            }
            for c in candles
        ],
    }


@router.get("/health")
async def health():
    return {"status": "ok"}

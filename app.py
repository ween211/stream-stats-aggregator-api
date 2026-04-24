from __future__ import annotations

from typing import Optional, Dict, Any, List, Literal
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from contextlib import asynccontextmanager
import asyncio
import random
import time as pytime
import os

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from dotenv import load_dotenv

load_dotenv()

# ===================== ENV / CONFIG =====================

EXTERNAL_API_KEY = os.getenv("EXTERNAL_API_KEY", "change_me")
STUDIO_USERNAME = os.getenv("STUDIO_USERNAME", "example_studio")

if EXTERNAL_API_KEY == "change_me" or STUDIO_USERNAME == "example_studio":
    raise RuntimeError("Fill EXTERNAL_API_KEY and STUDIO_USERNAME in .env")

MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "1"))
QUEUE_TIMEOUT = float(os.getenv("QUEUE_TIMEOUT", "900"))
POST_JOB_COOLDOWN = float(os.getenv("POST_JOB_COOLDOWN", "1.5"))
GLOBAL_MIN_INTERVAL = float(os.getenv("GLOBAL_MIN_INTERVAL", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.8"))
JITTER_MAX = float(os.getenv("JITTER_MAX", "0.3"))

BASE_URL = os.getenv("EXTERNAL_API_BASE_URL", "https://example.com")
PATH_TMPL = os.getenv(
    "EXTERNAL_API_PATH_TEMPLATE",
    "/api/stats/v2/studios/username/{studio}/models/username/{model}",
)

app = FastAPI(
    title="Stream Stats Aggregator API",
    version="1.0",
    description="FastAPI service for collecting, calculating and aggregating stream statistics from an external API.",
)

# ===================== CONCURRENCY / RATE LIMIT =====================

_sem = asyncio.Semaphore(MAX_CONCURRENCY)
_active = 0
_active_lock = asyncio.Lock()


@asynccontextmanager
async def concurrency_slot():
    """
    Limits concurrent jobs and returns 503 if the queue waits too long.
    """
    global _active

    try:
        await asyncio.wait_for(_sem.acquire(), timeout=QUEUE_TIMEOUT)
    except asyncio.TimeoutError:
        raise HTTPException(503, "Server is busy. Try again later.")

    async with _active_lock:
        _active += 1

    try:
        yield
    finally:
        try:
            await asyncio.sleep(POST_JOB_COOLDOWN)
        finally:
            async with _active_lock:
                _active -= 1
            _sem.release()


_rate_lock = asyncio.Lock()
_last_call_ts = 0.0


async def api_rate_gate():
    """
    Global interval between external API calls.
    """
    global _last_call_ts

    async with _rate_lock:
        now = pytime.monotonic()
        wait = (_last_call_ts + GLOBAL_MIN_INTERVAL) - now

        if wait > 0:
            await asyncio.sleep(wait)


def api_rate_after():
    global _last_call_ts
    _last_call_ts = pytime.monotonic()


# ===================== TIME HELPERS =====================

def to_utc_dt(value: str, *, end_of_day: bool, tz_name: str) -> datetime:
    """
    Converts date or datetime string to UTC datetime.

    If the value contains "T", it is treated as ISO 8601 datetime.
    If it has no timezone, tz_name is applied.

    If the value is only a date, it is converted to start or end of that day.
    """
    if "T" in value:
        parsed_value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(parsed_value)

        if dt.tzinfo is None:
            try:
                tz = ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                raise HTTPException(400, f"Unknown tz_name '{tz_name}'")

            dt = dt.replace(tzinfo=tz)

        return dt.astimezone(timezone.utc)

    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        raise HTTPException(400, f"Unknown tz_name '{tz_name}'")

    date_value = datetime.fromisoformat(value)
    target_time = time(23, 59, 59) if end_of_day else time(0, 0, 0)

    return datetime.combine(date_value, target_time).replace(tzinfo=tz).astimezone(timezone.utc)


def format_external_api_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_utc_response(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


# ===================== HTTP CLIENT =====================

async def call_external_api(model_username: str, params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "accept": "application/json",
        "API-key": EXTERNAL_API_KEY,
    }

    timeout = httpx.Timeout(25.0, connect=10.0)
    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        await api_rate_gate()

        try:
            async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
                response = await client.get(
                    PATH_TMPL.format(studio=STUDIO_USERNAME, model=model_username),
                    params=params,
                    headers=headers,
                )

            api_rate_after()

        except httpx.RequestError as exc:
            last_exc = exc

            retry = isinstance(
                exc,
                (
                    httpx.ConnectError,
                    httpx.ReadTimeout,
                    httpx.RemoteProtocolError,
                ),
            )

            if retry and attempt < MAX_RETRIES:
                await asyncio.sleep(BACKOFF_BASE ** attempt + random.uniform(0, JITTER_MAX))
                continue

            raise HTTPException(502, f"Network error to external API: {exc!r}")

        if response.status_code == 429:
            last_exc = HTTPException(429, response.text or "Too many requests")

            if attempt < MAX_RETRIES:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else BACKOFF_BASE ** attempt
                await asyncio.sleep(wait + random.uniform(0, JITTER_MAX))
                continue

            raise last_exc

        if response.status_code in (500, 502, 503, 504):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(BACKOFF_BASE ** attempt + random.uniform(0, JITTER_MAX))
                continue

            raise HTTPException(
                502,
                f"External API upstream error {response.status_code}: {response.text}",
            )

        if response.status_code >= 400:
            raise HTTPException(
                response.status_code,
                f"External API error {response.status_code}: {response.text}",
            )

        try:
            return response.json()
        except Exception:
            raise HTTPException(502, "External API returned non-JSON response")

    raise last_exc or HTTPException(502, "Unknown error calling external API")


# ===================== PARSERS =====================

def parse_tokens_from_response(data: Dict[str, Any]) -> Optional[float]:
    if not isinstance(data, dict):
        return None

    if "totalEarnings" in data and isinstance(data["totalEarnings"], (int, float)):
        return float(data["totalEarnings"])

    excluded_keys = {"periodStart", "periodEnd"}
    total = 0.0
    refunds = 0.0
    found = False

    for key, value in data.items():
        if key in excluded_keys:
            continue

        if isinstance(value, (int, float)):
            if key.lower() == "refunds":
                refunds += float(value)
            else:
                total += float(value)

            found = True

    return total - refunds if found else None


def sum_selected_categories(
    data: Dict[str, Any],
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> Optional[float]:
    if not isinstance(data, dict):
        return None

    excluded_keys = set(exclude or [])
    excluded_keys.update({"periodStart", "periodEnd"})

    keys = include if include else [
        key for key, value in data.items()
        if isinstance(value, (int, float))
    ]

    total = 0.0
    refunds = 0.0
    found = False

    for key in keys:
        if key in excluded_keys:
            continue

        value = data.get(key)

        if isinstance(value, (int, float)):
            if key.lower() == "refunds":
                refunds += float(value)
            else:
                total += float(value)

            found = True

    return total - refunds if found else None


# ===================== PYDANTIC MODELS =====================

class BaseSchema(BaseModel):
    model_config = ConfigDict(protected_namespaces=())


class TokensWindowRequest(BaseSchema):
    model_username: str
    period_start: str
    period_end: str
    tz_name: str = "Europe/Moscow"


class TokensWindowResponse(BaseSchema):
    studio: str
    model: str
    start_utc: str
    end_utc: str
    tokens: Optional[float] = None
    raw: Optional[Dict[str, Any]] = None


class BonusRequest(BaseSchema):
    model_username: str
    prev_stream_end: str
    next_stream_start: str
    tz_name: str = "Europe/Moscow"
    mode: Literal["time", "categories"] = "time"
    include_categories: Optional[List[str]] = None
    exclude_categories: Optional[List[str]] = None


class BonusResponse(BaseSchema):
    studio: str
    model: str
    start_utc: str
    end_utc: str
    bonus_tokens: Optional[float]
    mode: str
    categories_used: Optional[List[str]] = None
    raw: Optional[Dict[str, Any]] = None


class StreamAndBonusRequest(BaseSchema):
    model_username: str
    period_start: str
    period_end: str
    prev_stream_end: str
    tz_name: str = "Europe/Moscow"
    bonus_mode: Literal["time", "categories"] = "time"
    include_categories: Optional[List[str]] = None
    exclude_categories: Optional[List[str]] = None


class StreamPart(BaseSchema):
    start_utc: str
    end_utc: str
    tokens: Optional[float] = None


class BonusPart(BaseSchema):
    start_utc: str
    end_utc: str
    tokens: Optional[float] = None
    mode: str
    categories_used: Optional[List[str]] = None


class StreamAndBonusResponse(BaseSchema):
    studio: str
    model: str
    stream: StreamPart
    bonus: BonusPart
    total_tokens: Optional[float] = None
    raw_stream: Optional[Dict[str, Any]] = None
    raw_bonus: Optional[Dict[str, Any]] = None


# ===================== ROUTES =====================

@app.get("/")
def index():
    return {
        "ok": True,
        "service": "Stream Stats Aggregator API",
        "docs": "/docs",
        "health": "/healthz",
    }


@app.get("/healthz")
async def health():
    return {"ok": True}


@app.get("/queue-stats")
async def queue_stats():
    return {
        "max_concurrency": MAX_CONCURRENCY,
        "active": _active,
        "capacity_left": max(0, MAX_CONCURRENCY - _active),
    }


@app.post("/stats/tokens-by-window", response_model=TokensWindowResponse)
async def tokens_by_window(body: TokensWindowRequest, debug: bool = Query(False)):
    async with concurrency_slot():
        start = to_utc_dt(body.period_start, end_of_day=False, tz_name=body.tz_name)
        end = to_utc_dt(body.period_end, end_of_day=True, tz_name=body.tz_name)

        if end <= start:
            raise HTTPException(400, "period_end must be later than period_start")

        data = await call_external_api(
            body.model_username,
            {
                "periodStart": format_external_api_dt(start),
                "periodEnd": format_external_api_dt(end),
            },
        )

        tokens = parse_tokens_from_response(data)

        return {
            "studio": STUDIO_USERNAME,
            "model": body.model_username,
            "start_utc": format_utc_response(start),
            "end_utc": format_utc_response(end),
            "tokens": tokens,
            "raw": data if debug else None,
        }


@app.post("/stats/bonus-between", response_model=BonusResponse)
async def bonus_between(body: BonusRequest, debug: bool = Query(False)):
    async with concurrency_slot():
        start = to_utc_dt(body.prev_stream_end, end_of_day=False, tz_name=body.tz_name)
        end = to_utc_dt(body.next_stream_start, end_of_day=False, tz_name=body.tz_name)

        if end <= start:
            raise HTTPException(400, "next_stream_start must be later than prev_stream_end")

        data = await call_external_api(
            body.model_username,
            {
                "periodStart": format_external_api_dt(start),
                "periodEnd": format_external_api_dt(end),
            },
        )

        if body.mode == "time":
            tokens = parse_tokens_from_response(data)
            categories = None
        else:
            tokens = sum_selected_categories(
                data,
                include=body.include_categories,
                exclude=body.exclude_categories,
            )
            categories = body.include_categories

        return {
            "studio": STUDIO_USERNAME,
            "model": body.model_username,
            "start_utc": format_utc_response(start),
            "end_utc": format_utc_response(end),
            "bonus_tokens": tokens,
            "mode": body.mode,
            "categories_used": categories,
            "raw": data if debug else None,
        }


@app.post("/stats/stream-and-bonus", response_model=StreamAndBonusResponse)
async def stream_and_bonus(body: StreamAndBonusRequest, debug: bool = Query(False)):
    async with concurrency_slot():
        stream_start = to_utc_dt(body.period_start, end_of_day=False, tz_name=body.tz_name)
        stream_end = to_utc_dt(body.period_end, end_of_day=False, tz_name=body.tz_name)

        if stream_end <= stream_start:
            raise HTTPException(400, "period_end must be later than period_start")

        bonus_start = to_utc_dt(body.prev_stream_end, end_of_day=False, tz_name=body.tz_name)
        bonus_end = stream_start

        if bonus_end <= bonus_start:
            raise HTTPException(
                400,
                "period_start must be later than prev_stream_end for bonus calculation",
            )

        stream_raw = await call_external_api(
            body.model_username,
            {
                "periodStart": format_external_api_dt(stream_start),
                "periodEnd": format_external_api_dt(stream_end),
            },
        )
        stream_tokens = parse_tokens_from_response(stream_raw)

        bonus_raw = await call_external_api(
            body.model_username,
            {
                "periodStart": format_external_api_dt(bonus_start),
                "periodEnd": format_external_api_dt(bonus_end),
            },
        )

        if body.bonus_mode == "time":
            bonus_tokens = parse_tokens_from_response(bonus_raw)
            categories = None
        else:
            bonus_tokens = sum_selected_categories(
                bonus_raw,
                include=body.include_categories,
                exclude=body.exclude_categories,
            )
            categories = body.include_categories

        total_tokens = (
            (stream_tokens or 0.0) + (bonus_tokens or 0.0)
            if stream_tokens is not None or bonus_tokens is not None
            else None
        )

        return {
            "studio": STUDIO_USERNAME,
            "model": body.model_username,
            "stream": {
                "start_utc": format_utc_response(stream_start),
                "end_utc": format_utc_response(stream_end),
                "tokens": stream_tokens,
            },
            "bonus": {
                "start_utc": format_utc_response(bonus_start),
                "end_utc": format_utc_response(bonus_end),
                "tokens": bonus_tokens,
                "mode": body.bonus_mode,
                "categories_used": categories,
            },
            "total_tokens": total_tokens,
            "raw_stream": stream_raw if debug else None,
            "raw_bonus": bonus_raw if debug else None,
        }


# ===================== LOCAL RUN =====================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8080, reload=False)

import time
from typing import Optional

from fastapi import APIRouter, Request, Query
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/liquidations",
    tags=["Liquidations"]
)

@router.get('/')
async def get_recent_liquidations(
        request: Request,
        limit: int = Query(50, description="Количество записей", le=500)
):
    pool = request.app.state.db_pool
    query = """
            SELECT trade_id, coin, price, size, usd_amount, side, liq_type, timestamp, nonce 
            FROM liqs 
            ORDER BY timestamp DESC 
            LIMIT $1;
        """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ Ошибка при получении ликвидаций из БД: {e}")
        return {"error": "Internal server error", "details": str(e)}


@router.get("/history")
async def get_liquidation_history(
        request: Request,
        sort_order: str = Query("desc", enum=["asc", "desc"]),
        period: str = Query("all", enum=["24h", "all"]),
        symbol: str = Query("ALL"),
        limit: int = Query(50, le=500),
        offset: int = Query(0)
):
    pool = request.app.state.db_pool

    # 1. Формируем WHERE фильтры и список аргументов
    where_parts = []
    query_args = []

    # Фильтр по монете
    if symbol != "ALL":
        query_args.append(symbol)
        where_parts.append(f"coin = ${len(query_args)}")

    # Фильтр по времени (24 часа)
    if period == "24h":
        day_ago_ms = int(time.time() * 1000) - 86400000
        query_args.append(day_ago_ms)
        where_parts.append(f"timestamp > ${len(query_args)}")

    where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""

    # 2. Определяем сортировку (основная магия тут)
    # Всегда добавляем timestamp DESC вторым параметром, чтобы при одинаковых суммах была логика времени
    order_by = f"usd_amount {sort_order.upper()}, timestamp DESC"

    # 3. Собираем финальный запрос
    # Нам нужно еще добавить limit и offset в аргументы
    query_args.append(limit)
    limit_idx = len(query_args)

    query_args.append(offset)
    offset_idx = len(query_args)

    query = f"""
        SELECT trade_id, coin, price, size, usd_amount, side, liq_type, timestamp, nonce 
        FROM liqs 
        {where_clause}
        ORDER BY {order_by}
        LIMIT ${limit_idx} OFFSET ${offset_idx};
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *query_args)
            return {
                "success": True,
                "data": [dict(row) for row in rows]
            }
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки истории ликвидаций: {e}")
        return {"success": False, "error": str(e)}


@router.get("/summary")
async def get_liquidation_summary(request: Request):
    pool = request.app.state.db_pool
    # 24 часа назад в миллисекундах
    day_ago_ms = int(time.time() * 1000) - 86400000

    query = """
        SELECT 
            COALESCE(SUM(usd_amount), 0) as total_usd,
            COUNT(*) as total_count,
            COALESCE(SUM(CASE WHEN side = 'close_long' THEN usd_amount ELSE 0 END), 0) as long_usd,
            COALESCE(SUM(CASE WHEN side = 'close_short' THEN usd_amount ELSE 0 END), 0) as short_usd
        FROM liqs
        WHERE timestamp > $1;
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, day_ago_ms)
            return {"success": True, "data": dict(row)}
    except Exception as e:
        logger.error(f"❌ Ошибка статистики: {e}")
        return {"success": False, "error": str(e)}
from fastapi import APIRouter, Request, Query
from typing import List
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
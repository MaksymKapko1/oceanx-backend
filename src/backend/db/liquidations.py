import logging
import asyncpg
import time

logger = logging.getLogger(__name__)

async def insert_liquidation(pool: asyncpg.Pool, liq_data: dict):
    query = """
    INSERT INTO liqs (trade_id, coin, price, size, usd_amount, side, liq_type, timestamp, nonce)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (trade_id) DO NOTHING;
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                liq_data["trade_id"], liq_data["coin"], liq_data["price"],
                liq_data["size"], liq_data["usd_amount"], liq_data["side"],
                liq_data["liq_type"], liq_data["timestamp"], liq_data["nonce"]
            )
    except Exception as e:
        logger.error(f"⚠️ Ошибка записи ликвидации {liq_data.get('trade_id')} в БД: {e}")

async def get_24h_liquidations_volume(pool: asyncpg.Pool) -> float:
    twenty_four_hours_ago = int(time.time()  * 1000) - 86400000
    query = """
    SELECT SUM(usd_amount)
    FROM liqs
    WHERE timestamp > $1;
    """
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchval(query, twenty_four_hours_ago)
            return float(result) if result else 0.0
    except Exception as e:
        logger.error(f"⚠️ Ошибка при подсчете ликвидаций за 24ч: {e}")
        return 0.0

async def get_historical_liquidations(pool: asyncpg.Pool) -> list[dict]:
    query = """
    SELECT 
        (CAST(timestamp / 86400000 AS BIGINT) * 86400000) AS day_ts,
        SUM(usd_amount) AS daily_usd
    FROM liqs
    GROUP BY day_ts
    ORDER BY day_ts ASC;
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [{"time": row["day_ts"], "value": row["daily_usd"]} for row in rows]
    except Exception as e:
        logger.error(f"⚠️ Ошибка чтения истории ликвидаций: {e}")
        return []
import logging
import asyncpg

logger = logging.getLogger(__name__)

async def insert_daily_volume(pool: asyncpg.Pool, timestamp: int, usd_volume: float):
    query = """
    INSERT INTO volume (timestamp, usd_volume)
    VALUES ($1, $2)
    ON CONFLICT (timestamp) 
    DO UPDATE SET usd_volume = EXCLUDED.usd_volume;
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                timestamp,
                usd_volume
            )
            logger.debug(f"📊 Volume per {timestamp} saved: ${usd_volume:,.2f}")
    except Exception as e:
        logger.error(f"⚠️ Error recording the volume for {timestamp} in DB: {e}")

async def get_historical_volume(pool: asyncpg.Pool) -> list[dict]:
    query = """
    SELECT timestamp, usd_volume 
    FROM volume 
    ORDER BY timestamp ASC;
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [{"time": row["timestamp"], "value": row["usd_volume"]} for row in rows]
    except Exception as e:
        logger.error(f"⚠️ Error reading volume history from the database: {e}")
        return []
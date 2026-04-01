import logging
import asyncpg

logger = logging.getLogger(__name__)

async def init_db(pool: asyncpg.Pool):
    # 1. Таблица ликвидаций
    query_create_liqs_table = """
    CREATE TABLE IF NOT EXISTS liqs (
        trade_id BIGINT PRIMARY KEY,
        coin VARCHAR(50) NOT NULL,
        price DOUBLE PRECISION NOT NULL,
        size DOUBLE PRECISION NOT NULL,
        usd_amount DOUBLE PRECISION NOT NULL,
        side VARCHAR(30) NOT NULL,
        liq_type VARCHAR(50) NOT NULL,
        timestamp BIGINT NOT NULL,
        nonce BIGINT
    );
    """

    # 2. Таблица исторических объемов (ИСПРАВЛЕНО)
    query_create_volume_table = """
    CREATE TABLE IF NOT EXISTS volume (
        timestamp BIGINT PRIMARY KEY,
        usd_volume DOUBLE PRECISION NOT NULL
    );
    """

    # 3. Индексы (Оставили только нужные)
    queries_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_liqs_timestamp ON liqs (timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_liqs_coin ON liqs (coin);",
        "CREATE INDEX IF NOT EXISTS idx_liqs_usd_amount ON liqs (usd_amount DESC);"
        # Индекс для volume(timestamp) убрали, так как PRIMARY KEY делает это автоматически
    ]

    try:
        async with pool.acquire() as conn:
            # Выполняем создание ОБЕИХ таблиц
            await conn.execute(query_create_liqs_table)
            await conn.execute(query_create_volume_table)

            # Накатываем индексы
            for q_idx in queries_indexes:
                await conn.execute(q_idx)

        logger.info("✅ База данных (liqs, volume) и индексы успешно инициализированы")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при инициализации БД: {e}")
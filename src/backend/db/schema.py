import logging
import asyncpg

logger = logging.getLogger(__name__)

async def init_db(pool: asyncpg.Pool):
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

    query_create_volume_table = """
    CREATE TABLE IF NOT EXISTS volume (
        timestamp BIGINT PRIMARY KEY,
        usd_volume DOUBLE PRECISION NOT NULL
    );
    """

    query_create_user_table = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(256) UNIQUE NOT NULL,
        builder_approved BOOLEAN DEFAULT FALSE,
        pacifica_api_key VARCHAR(256),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """

    query_create_subscriptions_table = """
       CREATE TABLE IF NOT EXISTS subscriptions (
           id SERIAL PRIMARY KEY,
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           master_wallet VARCHAR(255) NOT NULL,
           is_active BOOLEAN DEFAULT TRUE,
           is_reverse BOOLEAN DEFAULT FALSE,
           copy_amount NUMERIC(10, 2),
           max_leverage INTEGER DEFAULT 10,
           created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
           updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
           
           UNIQUE (user_id, master_wallet)
       );
       """

    query_create_copied_trades_table = """
       CREATE TABLE IF NOT EXISTS copied_trades (
            id SERIAL PRIMARY KEY,
            subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
            master_trade_id VARCHAR(255), 
            master_size NUMERIC(18, 6) DEFAULT 0,
            follower_trade_id VARCHAR(255), 
            symbol VARCHAR(50) NOT NULL, 
            side VARCHAR(20) NOT NULL,
            entry_price NUMERIC(18, 6),
            size NUMERIC(18, 6), 
            status VARCHAR(50) DEFAULT 'open', -- open, closed, failed
            error_log TEXT, 
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
       );
       """

    query_create_agent_wallets_table = """
            CREATE TABLE IF NOT EXISTS agent_wallets (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                public_key VARCHAR(256) NOT NULL,
                encrypted_private_key TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_active_agent UNIQUE (user_id)
            );
        """

    query_create_user_risk_settings_table = """
        CREATE TABLE IF NOT EXISTS user_risk_settings (
        id SERIAL PRIMARY KEY,
        user_id INTEGER UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        volume_per_trade_usd NUMERIC(18, 2) NOT NULL DEFAULT 50.00,
        max_slippage NUMERIC(5, 2) NOT NULL,
        max_total_exposure_usd NUMERIC(18, 2) DEFAULT 500.00,
        allowed_markets VARCHAR(20)[] DEFAULT '{}',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    queries_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_liqs_timestamp ON liqs (timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_liqs_coin ON liqs (coin);",
        "CREATE INDEX IF NOT EXISTS idx_liqs_usd_amount ON liqs (usd_amount DESC);",
        "CREATE INDEX IF NOT EXISTS idx_users_wallet ON users(wallet_address);",
        "CREATE INDEX IF NOT EXISTS idx_subs_user_master ON subscriptions(user_id, master_wallet);",
        "CREATE INDEX IF NOT EXISTS idx_copied_trades_sub ON copied_trades(subscription_id);",
        "CREATE INDEX IF NOT EXISTS idx_agent_wallets_user_id ON agent_wallets(user_id);"
    ]

    try:
        async with pool.acquire() as conn:
            await conn.execute(query_create_liqs_table)
            await conn.execute(query_create_volume_table)
            await conn.execute(query_create_user_table)
            await conn.execute(query_create_subscriptions_table)
            await conn.execute(query_create_copied_trades_table)
            await conn.execute(query_create_agent_wallets_table)
            await conn.execute(query_create_user_risk_settings_table)

            for q_idx in queries_indexes:
                await conn.execute(q_idx)

        logger.info("✅ The database (liqs, volume, users, subs, copied_trades, agents, risk table) and indexes have been successfully initialized")
    except Exception as e:
        logger.error(f"❌ Critical error during database initialization: {e}")
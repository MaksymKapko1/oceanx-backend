from typing import List

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/user", tags=["User"])

class RiskSettingsUser(BaseModel):
    user_wallet:str
    margin_allocation: float
    leverage: float
    slippage: float
    allowed_markets: List[str] = []
    max_total_exposure_usd: float = 500.0

class UpdateStrategyRequest(BaseModel):
    user_wallet: str
    master_wallet: str
    is_reverse: bool

@router.post("/update-strategy")
async def update_strategy(req: UpdateStrategyRequest, request: Request):
    pool = request.app.state.db_pool

    query = """
            UPDATE subscriptions 
            SET is_reverse = $3, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = (SELECT id FROM users WHERE wallet_address = $1)
              AND master_wallet = $2
            RETURNING id, is_reverse;
        """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, req.user_wallet, req.master_wallet, req.is_reverse)
            if not row:
                raise HTTPException(status_code=404, detail="Subscription not found")

            logger.info(
                f"🔄 Стратегия изменена для {req.user_wallet[:6]} -> {req.master_wallet[:6]}: Reverse={req.is_reverse}")
            return {"success": True, "is_reverse": row['is_reverse']}
    except Exception as e:
        logger.error(f"Ошибка смены стратегии: {e}")
        return {"success": False, "error": str(e)}

@router.post("/settings")
async def save_risk_settings(req: RiskSettingsUser, request: Request):
    pool = request.app.state.db_pool

    query = """
        INSERT INTO user_risk_settings (
                user_id, 
                margin_allocation_pct, 
                max_leverage, 
                max_slippage, 
                allowed_markets,
                max_total_exposure_usd,
                updated_at
            )
            VALUES (
                (SELECT id FROM users WHERE wallet_address = $1), 
                $2, $3, $4, $5, $6, CURRENT_TIMESTAMP 
            )
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                margin_allocation_pct = EXCLUDED.margin_allocation_pct,
                max_leverage = EXCLUDED.max_leverage,
                max_slippage = EXCLUDED.max_slippage,
                allowed_markets = EXCLUDED.allowed_markets,
                max_total_exposure_usd = EXCLUDED.max_total_exposure_usd,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
    """

    try:
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                query,
                req.user_wallet,
                req.margin_allocation,
                req.leverage,
                req.slippage,
                req.allowed_markets,
                req.max_total_exposure_usd
            )

        if not result:
            raise HTTPException(status_code=404, detail="User not found")

        return {"success": True, "message": "Risk settings saved successfully"}
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения настроек для {req.user_wallet}: {e}")
        return {"success": False, "error": str(e)}


@router.get("/settings/{wallet}")
async def get_risk_settings(wallet: str, request: Request):
    pool = request.app.state.db_pool

    query = """
        SELECT margin_allocation_pct, max_leverage, max_slippage, allowed_markets, 
            COALESCE(max_total_exposure_usd, 500) as max_total_exposure_usd
        FROM user_risk_settings
        WHERE user_id = (SELECT id FROM users WHERE wallet_address = $1)
    """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, wallet)

            if row:
                return {
                    "success": True,
                    "settings": {
                        "margin_allocation_pct": row["margin_allocation_pct"],
                        "max_leverage": row["max_leverage"],
                        "max_slippage": float(row["max_slippage"]),
                        "allowed_markets": row["allowed_markets"] or [],
                        "max_total_exposure_usd": float(row["max_total_exposure_usd"])
                    }
                }

            return {"success": True, "settings": {}}

    except Exception as e:
        logger.error(f"❌ Ошибка выгрузки настроек для {wallet}: {e}")
        return {"success": False, "error": str(e)}
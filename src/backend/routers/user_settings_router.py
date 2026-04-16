from typing import List
from fastapi import Depends

from core.dependencies import get_active_wallet

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/user", tags=["User"])

class RiskSettingsUser(BaseModel):
    volume_per_trade_usd: float
    slippage: float
    allowed_markets: List[str] = []
    max_total_exposure_usd: float = 500.0

class UpdateStrategyRequest(BaseModel):
    master_wallet: str
    is_reverse: bool

class BuilderStatusUpdate(BaseModel):
    is_approved: bool

@router.post("/update-strategy")
async def update_strategy(req: UpdateStrategyRequest, request: Request, wallet: str = Depends(get_active_wallet)):
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
            row = await conn.fetchrow(query, wallet, req.master_wallet, req.is_reverse)
            if not row:
                raise HTTPException(status_code=404, detail="Subscription not found")

            logger.info(
                f"🔄 The strategy has been changed for {wallet[:6]} -> {req.master_wallet[:6]}: Reverse={req.is_reverse}")
            return {"success": True, "is_reverse": row['is_reverse']}
    except Exception as e:
        logger.error(f"The mistake of changing strategy: {e}")
        return {"success": False, "error": str(e)}

@router.post("/settings")
async def save_risk_settings(req: RiskSettingsUser, request: Request,wallet: str = Depends(get_active_wallet)):
    pool = request.app.state.db_pool

    query = """
        INSERT INTO user_risk_settings (
                user_id, 
                volume_per_trade_usd,
                max_slippage, 
                allowed_markets,
                max_total_exposure_usd,
                updated_at
            )
            VALUES (
                (SELECT id FROM users WHERE wallet_address = $1), 
                $2, $3, $4, $5, CURRENT_TIMESTAMP 
            )
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                volume_per_trade_usd = EXCLUDED.volume_per_trade_usd,
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
                wallet,
                req.volume_per_trade_usd,
                req.slippage,
                req.allowed_markets,
                req.max_total_exposure_usd
            )

        if not result:
            raise HTTPException(status_code=404, detail="User not found")

        return {"success": True, "message": "Risk settings saved successfully"}
    except Exception as e:
        logger.error(f"❌ Error saving settings for {wallet}: {e}")
        return {"success": False, "error": str(e)}


@router.get("/settings/{wallet}")
async def get_risk_settings(wallet: str, request: Request):
    pool = request.app.state.db_pool

    query = """
        SELECT volume_per_trade_usd, max_slippage, allowed_markets, 
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
                        "volume_per_trade_usd": float(row["volume_per_trade_usd"]),
                        "max_slippage": float(row["max_slippage"]),
                        "allowed_markets": row["allowed_markets"] or [],
                        "max_total_exposure_usd": float(row["max_total_exposure_usd"])
                    }
                }

            return {"success": True, "settings": {}}

    except Exception as e:
        logger.error(f"❌ Error unloading settings for {wallet}: {e}")
        return {"success": False, "error": str(e)}

@router.get("/builder-status")
async def get_builder_status(request: Request, wallet: str = Depends(get_active_wallet)):
    db_pool = request.app.state.db_pool

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT builder_approved 
            FROM users 
            WHERE wallet_address = $1
        """, wallet)

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return {"is_approved": row["builder_approved"] or False}

@router.post("/update-builder-status")
async def update_builder_status(req: BuilderStatusUpdate, request: Request,
                                wallet: str = Depends(get_active_wallet)):
    db_pool = request.app.state.db_pool

    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE users 
            SET builder_approved = $1 
            WHERE wallet_address = $2
        """, req.is_approved, wallet)

    return {"success": True, "message": "Builder status updated"}
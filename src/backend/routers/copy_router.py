import logging
from fastapi import APIRouter, Request, Body, HTTPException
from pydantic import BaseModel
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/copy",
    tags=["CopyTrading"]
)

class FollowRequest(BaseModel):
    user_wallet: str
    master_wallet: str
    copy_amount: Optional[float] = 100.00
    max_leverage: Optional[int] = 10
    is_reverse: bool = False

class UnfollowRequest(BaseModel):
    user_wallet: str
    master_wallet: str

class ToggleRequest(BaseModel):
    subscription_id: int
    is_active: bool

@router.post("/follow")
async def follow_master(req: FollowRequest, request: Request):
    pool = request.app.state.db_pool
    user_wallet = req.user_wallet
    master_wallet = req.master_wallet

    query = """
            INSERT INTO subscriptions (user_id, master_wallet, copy_amount, max_leverage, is_active)
            SELECT id, $2, $3, $4, TRUE FROM users WHERE wallet_address = $1
            ON CONFLICT (user_id, master_wallet) 
            DO UPDATE SET is_active = TRUE, is_reverse = $5, copy_amount = $3, max_leverage = $4, updated_at = CURRENT_TIMESTAMP
            RETURNING id, is_active;
        """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, user_wallet, master_wallet, req.copy_amount, req.max_leverage, req.is_reverse)
            if not row:
                raise HTTPException(status_code=404, detail="User not found in database")

            ws_listener = getattr(request.app.state, 'ws_listener', None)
            if ws_listener:
                await ws_listener.add_master_instantly(master_wallet)
            return {"success": True, "subscription": dict(row)}
    except Exception as e:
        logging.error(f"Failed to follow master wallet {e}")
        return {"success": False, "error": str(e)}

@router.post("/unfollow")
async def unfollow_master(req: UnfollowRequest, request: Request):
    pool = request.app.state.db_pool
    user_wallet = req.user_wallet
    master_wallet = req.master_wallet

    query = """
            UPDATE subscriptions
            SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = (SELECT id FROM users WHERE wallet_address = $1)
              AND master_wallet = $2
            RETURNING id, is_active;
        """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, user_wallet, master_wallet)
            if not row:
                raise HTTPException(status_code=404, detail="Subscription not found")

            ws_listener = getattr(request.app.state, 'ws_listener', None)
            if ws_listener:
                await ws_listener.remove_master_instantly(master_wallet)

            return {"success": True, "status": dict(row)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.post("/toggle")
async def toggle_subscription(req: ToggleRequest, request: Request):
    pool = request.app.state.db_pool

    query = """
        UPDATE subscriptions
        SET is_active = $2, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        RETURNING id, is_active, master_wallet;
    """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, req.subscription_id, req.is_active)
            if not row:
                raise HTTPException(status_code=404, detail="Subscription not found")

            ws_listener = getattr(request.app.state, 'ws_listener', None)
            if ws_listener:
                master_wallet = row['master_wallet']
                if req.is_active:
                    await ws_listener.add_master_instantly(master_wallet)
                else:
                    await ws_listener.remove_master_instantly(master_wallet)
            return {"success": True, "status": dict(row)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.get("/subscriptions/{wallet}")
async def get_user_subscriptions(wallet: str, request: Request):
    pool = request.app.state.db_pool

    query = """
            SELECT master_wallet, is_active
            FROM subscriptions
            WHERE user_id = (SELECT id FROM users WHERE wallet_address = $1)
        """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, wallet)
            subs = {row['master_wallet']: row['is_active'] for row in rows}
            return {"success": True, "subscriptions": subs}
    except Exception as e:
        return {"success": False, "error": str(e)}
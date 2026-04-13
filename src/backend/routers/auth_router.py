import logging
import httpx
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from solders.keypair import Keypair

from core.security import crypto_manager

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/auth",
    tags=["Authentication"]
)

class UserConnectReq(BaseModel):
    wallet_address: str

class SaveAgentKeyRequest(BaseModel):
    user_wallet: str
    agent_public_key: str
    agent_private_key: str
    signature: str
    timestamp: int

@router.post("/connect")
async def connect_user(req: UserConnectReq, request: Request):
    wallet = req.wallet_address

    pool = request.app.state.db_pool
    query = """
        INSERT INTO users (wallet_address)
        VALUES ($1)
        ON CONFLICT (wallet_address)
        DO UPDATE SET wallet_address = EXCLUDED.wallet_address
        RETURNING id, wallet_address, created_at;
    """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, wallet)
            return {"success": True, "user": dict(row)}
    except Exception as e:
        logging.error(f"❌ Ошибка записи юзера: {e}")
        return {"success": False, "error": str(e)}

@router.post("/save-agent")
async def save_agent_key(req: SaveAgentKeyRequest, request: Request):
    pacifica_url = "https://api.pacifica.fi/api/v1/agent/bind"
    payload = {
        "account": req.user_wallet,
        "signature": req.signature,
        "timestamp": req.timestamp,
        "expiry_window": 5000,
        "agent_wallet": req.agent_public_key
    }
    wallet = req.user_wallet
    agent_public_key = req.agent_public_key
    pool = request.app.state.db_pool

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(pacifica_url, json=payload, timeout=10.0)
            if resp.status_code != 200:
                logger.error(f"Pacifica error: {resp.text}")
                raise HTTPException(status_code=400, detail=f"Pacifica rejected: {resp.text}")
        except Exception as e:
            logger.error(f"Connection to Pacifica failed: {e}")
            raise HTTPException(status_code=500, detail="Could not connect to Pacifica")

    try:
        Keypair.from_base58_string(req.agent_private_key)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Private Key format")

    try:
        encrypted_pk = crypto_manager.encrypt_key(req.agent_private_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Encryption failed")


    query = """
        INSERT INTO agent_wallets (user_id, public_key, encrypted_private_key, is_active)
        SELECT id, $2, $3, TRUE FROM users WHERE wallet_address = $1
        ON CONFLICT (user_id) 
        DO UPDATE SET 
            public_key = EXCLUDED.public_key,
            encrypted_private_key = EXCLUDED.encrypted_private_key,
            is_active = TRUE
        RETURNING id;
    """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, wallet, agent_public_key, encrypted_pk)
        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return {"success": True, "message": "Agent Wallet securely saved"}
    except Exception as e:
        logger.error(f"DB Error saving agent key: {e}")
        return {"success": False, "error": str(e)}

@router.get("/status/{wallet}")
async def get_auth_status(wallet: str, request: Request):
    pool = request.app.state.db_pool

    query = """
            SELECT EXISTS (
                SELECT 1 FROM agent_wallets a
                JOIN users u ON a.user_id = u.id
                WHERE u.wallet_address = $1 AND a.is_active = TRUE
            );
        """

    try:
        async with pool.acquire() as conn:
            has_agent = await conn.fetchval(query, wallet)

        return {"success": True, "wallet": wallet, "has_agent": has_agent}
    except Exception as e:
        logger.error(f"Ошибка проверки статуса агента для {wallet}: {e}")
        return {"success": False, "error": str(e)}
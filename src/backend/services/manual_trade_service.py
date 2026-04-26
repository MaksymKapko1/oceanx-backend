import asyncio
import json
import math
import time
import uuid
import logging
import os

import aiohttp
import websockets
from cryptography.fernet import Fernet
from solders.keypair import Keypair

from common.utils import sign_message
from common.constants import WS_URL
from services.pacifica_client import pacifica_client

from common.constants import REST_URL

logger = logging.getLogger(__name__)

class ManualTradeService:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        encryption_key = os.getenv("MASTER_KEY")
        if encryption_key:
            self.fernet = Fernet(encryption_key.encode())
        else:
            logger.error("CRITICAL: MASTER_KEY IS NOT IN .env!")

    def _decrypt_key(self, encrypted_key: str) -> str:
        return self.fernet.decrypt(encrypted_key.encode()).decode()

    async def _get_agent_wallet(self, wallet: str):
        """We retrieve the agent wallet and the slippage settings from the database."""
        query = """
            SELECT a.public_key, a.encrypted_private_key, r.max_slippage, u.builder_approved
            FROM users u
            JOIN agent_wallets a ON u.id = a.user_id
            JOIN user_risk_settings r ON u.id = r.user_id
            WHERE u.wallet_address = $1 AND a.is_active = TRUE
        """
        async with self.db_pool.acquire() as conn:
            return await conn.fetchrow(query, wallet)

    async def close_position(self, wallet: str, symbol: str):
        """Closes a specific user session. VIA WEBSOCKET CONNECTION"""
        logger.info(f"🛑 Запрос на ручное закрытие {symbol} для {wallet[:6]}")

        agent_data = await self._get_agent_wallet(wallet)
        if not agent_data:
            return {"success": False, "error": "Agent wallet not found"}

        positions = await pacifica_client.fetch_user_positions(wallet)
        if symbol not in positions:
            return {"success": False, "error": f"No open position for {symbol}"}

        pos = positions[symbol]
        amount = pos['amount']
        current_side = pos['side']

        api_side = "ask" if current_side == "bid" else "bid"

        amount_str = f"{amount:f}".rstrip('0').rstrip('.') #DELETE 0s in pos

        agent_pub = agent_data['public_key']
        agent_priv_str = self._decrypt_key(agent_data['encrypted_private_key'])
        max_slippage = agent_data['max_slippage'] or 1.0

        builder_approved = agent_data.get('builder_approved', False)

        success, err_msg = await self._send_ws_order(
            wallet, agent_pub, agent_priv_str, symbol, api_side, amount_str, max_slippage, builder_approved
        )

        if success:
            logger.info(f"✅ Manual closure of {symbol} was successful for {wallet[:6]}")
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE copied_trades 
                        SET status = 'closed' 
                        WHERE subscription_id IN (
                            SELECT id FROM subscriptions WHERE user_id = (SELECT id FROM users WHERE wallet_address = $1)
                        ) AND symbol = $2
                    """, wallet, symbol)
            except Exception as e:
                logger.error(f"Database update error when closing manually: {e}")

            return {"success": True, "message": f"{symbol} position closed"}
        else:
            logger.error(f"❌ Manual closure error {symbol}: {err_msg}")
            return {"success": False, "error": err_msg}


    async def close_all_positions(self, wallet: str):
        """Closes all open positions in the wallet. VIA WEBSOCKET CONNECTION"""
        logger.info(f"☢️Request to close ALL positions for {wallet[:6]}")
        positions = await pacifica_client.fetch_user_positions(wallet)

        if not positions:
            return {"success": True, "message": "No open positions"}

        tasks = [self.close_position(wallet, symbol) for symbol in positions.keys()]
        results = await asyncio.gather(*tasks)

        success_count = sum(1 for r in results if r.get("success"))
        return {
            "success": True,
            "message": f"Closed {success_count}/{len(positions)} positions",
            "details": results
        }

    async def _send_ws_order(self, wallet, agent_pub, agent_priv_str, symbol, api_side,
                             amount_str, slippage, builder_approved):
        """Generates a signature and sends a Reduce-Only order via WebSocket."""
        keypair = Keypair.from_base58_string(agent_priv_str)
        timestamp = int(time.time() * 1000)
        client_order_id = str(uuid.uuid4())

        signature_payload = {
            "symbol": symbol,
            "reduce_only": True,
            "amount": amount_str,
            "side": api_side,
            "slippage_percent": str(slippage),
            "client_order_id": client_order_id,
        }
        if builder_approved:
            signature_payload["builder_code"] = "redwingss"

        _, signature = sign_message(
            {"timestamp": timestamp, "expiry_window": 5000, "type": "create_market_order"},
            signature_payload,
            keypair
        )

        ws_message = {
            "id": str(uuid.uuid4()),
            "params": {"create_market_order": {
                "account": wallet,
                "agent_wallet": agent_pub,
                "signature": signature,
                "timestamp": timestamp,
                "expiry_window": 5000,
                **signature_payload
            }},
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with websockets.connect(WS_URL) as websocket:
                    await websocket.send(json.dumps(ws_message))
                    response = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5.0))

                    if response.get("code") == 200:
                        return True, ""
                    else:
                        return False, response.get('err', 'Unknown API error')
            except Exception as e:
                logger.warning(f"⚠️ WS attempt {attempt + 1}/{max_retries} to close failed: {e}")
                await asyncio.sleep(0.5)

        return False, "WebSocket timeout / connection failed"

    #--------------------------
    #REST API
    #--------------------------
    async def open_position(self, wallet: str, symbol: str, side: str, size_usd: float):
        logger.info(f"🟢 [REST] Запрос на открытие {side.upper()} {symbol} на ${size_usd} для {wallet[:6]}")

        agent_data = await self._get_agent_wallet(wallet)
        if not agent_data:
            return {"success": False, "error": "Agent wallet not found"}

        market_info = pacifica_client.cache.get('market_info', {}).get(symbol)
        mark_price = next(
            (m['mark_price'] for m in pacifica_client.cache.get('top_volume', []) if m['symbol'] == symbol), 0)

        if not market_info or mark_price <= 0:
            return {"success": False, "error": f"Market info or price not available for {symbol}"}

        raw_amount = size_usd / mark_price
        lot_size = market_info['lot_size']
        formatted_size = round(math.floor(raw_amount / lot_size) * lot_size, 8)

        if formatted_size <= 0:
            return {"success": False, "error": "Order size too small"}

        amount_str = f"{formatted_size:f}".rstrip('0').rstrip('.')
        api_side = "bid" if side == "long" else "ask"

        agent_pub = agent_data['public_key']
        agent_priv_str = self._decrypt_key(agent_data['encrypted_private_key'])
        keypair = Keypair.from_base58_string(agent_priv_str)
        max_slippage = str(agent_data['max_slippage'] or 1.0)
        builder_code = "redwingss" if agent_data.get('builder_approved') else None #TODO добавить норм проверку если не подписались на код
        timestamp = int(time.time() * 1000)

        payload = {
            "symbol": symbol,
            "amount": amount_str,
            "side": api_side,
            "slippage_percent": max_slippage,
            "reduce_only": False,
            "client_order_id": str(uuid.uuid4())
        }

        if builder_code:
            payload["builder_code"] = builder_code

        _, signature = sign_message(
            {"timestamp": timestamp, "expiry_window": 5000, "type": "create_market_order"},
            payload, keypair
        )

        request_body = {
            "account": wallet,
            "agent_wallet": agent_pub,
            "signature": signature,
            "timestamp": timestamp,
            "expiry_window": 5000,
            **payload
        }

        url = f"{REST_URL}/orders/create_market"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=request_body) as resp:
                    resp_data = await resp.json()
                    if resp.status == 200 and ("order_id" in resp_data or resp_data.get("success")):
                        return {"success": True, "message": f"Opened {side} on {symbol}"}
                    else:
                        err = resp_data.get('error') or resp_data
                        return {"success": False, "error": str(err)}
        except Exception as e:
            return {"success": False, "error": "Network error while sending order"}

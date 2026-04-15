import asyncio
import json
import time
import uuid
import logging
import os
import websockets
from cryptography.fernet import Fernet
from solders.keypair import Keypair

from common.utils import sign_message
from common.constants import WS_URL
from services.pacifica_client import pacifica_client

logger = logging.getLogger(__name__)


class ManualTradeService:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        encryption_key = os.getenv("MASTER_KEY")
        if encryption_key:
            self.fernet = Fernet(encryption_key.encode())
        else:
            logger.error("КРИТИЧЕСКИ: MASTER_KEY не найден в .env!")

    def _decrypt_key(self, encrypted_key: str) -> str:
        return self.fernet.decrypt(encrypted_key.encode()).decode()

    async def _get_agent_wallet(self, wallet: str):
        """Достаем агентский кошелек и настройки проскальзывания из БД."""
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
        """Закрывает одну конкретную позицию пользователя."""
        logger.info(f"🛑 Запрос на ручное закрытие {symbol} для {wallet[:6]}")

        agent_data = await self._get_agent_wallet(wallet)
        if not agent_data:
            return {"success": False, "error": "Agent wallet not found"}

        # 1. Получаем реальную позу с биржи
        positions = await pacifica_client.fetch_user_positions(wallet)
        if symbol not in positions:
            return {"success": False, "error": f"No open position for {symbol}"}

        pos = positions[symbol]
        amount = pos['amount']
        current_side = pos['side']  # 'bid' (long) или 'ask' (short)

        # 2. Реверс стороны: если был bid, закрываем через ask
        api_side = "ask" if current_side == "bid" else "bid"

        # Форматируем объем без лишних нулей
        amount_str = f"{amount:f}".rstrip('0').rstrip('.')

        agent_pub = agent_data['public_key']
        agent_priv_str = self._decrypt_key(agent_data['encrypted_private_key'])
        max_slippage = agent_data['max_slippage'] or 1.0  # Дефолт если вдруг пусто

        builder_approved = agent_data.get('builder_approved', False)

        # 3. Отправляем ордер
        success, err_msg = await self._send_ws_order(
            wallet, agent_pub, agent_priv_str, symbol, api_side, amount_str, max_slippage, builder_approved
        )

        if success:
            logger.info(f"✅ Ручное закрытие {symbol} успешно для {wallet[:6]}")
            # Очищаем базу, чтобы бот не думал, что поза все еще открыта
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
                logger.error(f"Ошибка обновления БД при ручном закрытии: {e}")

            return {"success": True, "message": f"{symbol} position closed"}
        else:
            logger.error(f"❌ Ошибка ручного закрытия {symbol}: {err_msg}")
            return {"success": False, "error": err_msg}

    async def close_all_positions(self, wallet: str):
        """Закрывает все открытые позиции кошелька."""
        logger.info(f"☢️ ПАНИКА! Запрос на закрытие ВСЕХ позиций для {wallet[:6]}")
        positions = await pacifica_client.fetch_user_positions(wallet)

        if not positions:
            return {"success": True, "message": "No open positions"}

        # Запускаем закрытие всех позиций параллельно
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
        """Генерирует подпись и отправляет Reduce-Only ордер по WebSocket."""
        keypair = Keypair.from_base58_string(agent_priv_str)
        timestamp = int(time.time() * 1000)
        client_order_id = str(uuid.uuid4())

        signature_payload = {
            "symbol": symbol,
            "reduce_only": True,  # 🔥 САМОЕ ВАЖНОЕ - защита от открытия новой позы
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
                logger.warning(f"⚠️ WS попытка {attempt + 1}/{max_retries} закрытия не удалась: {e}")
                await asyncio.sleep(0.5)

        return False, "WebSocket timeout / connection failed"
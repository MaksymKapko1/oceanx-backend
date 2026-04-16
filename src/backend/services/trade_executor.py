import asyncio
import json
import time
import uuid
import logging
import os
import websockets
import math
from cryptography.fernet import Fernet
from solders.keypair import Keypair

from common.utils import sign_message
from common.constants import WS_URL
from services.pacifica_client import pacifica_client

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# SUPPORT UTILITY
# ─────────────────────────────────────────────

def format_lot_size(size: float, lot_size: float) -> float:
    if lot_size <= 0:
        return size
    steps = math.floor(size / lot_size) #0.32452 / 0.00001 = 32452
    return round(steps * lot_size, 8) #32452 * 0.00001 = 0.32452

# ─────────────────────────────────────────────
# INITIALIZATION AND BACKGROUND TASKS
# ─────────────────────────────────────────────

class CopyTradeExecutor:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.subscription_locks = {}
        self.rate_limit = asyncio.Semaphore(20)

        encryption_key = os.getenv("MASTER_KEY")
        if encryption_key:
            self.fernet = Fernet(encryption_key.encode())
        else:
            logger.error("КРИТИЧЕСКИ: MASTER_KEY не найден в .env!")

    def _decrypt_key(self, encrypted_key: str) -> str:
        return self.fernet.decrypt(encrypted_key.encode()).decode()

# ─────────────────────────────────────────────
#SIGNAL DISTRIBUTION
# ─────────────────────────────────────────────

    async def process_master_signal(self, master_wallet: str, symbol: str, side: str,
                                    master_price: float, master_amount: float):

        """Receives a signal from the administrator and distributes tasks to all subscribers."""
        logger.info(f"⚙️ EXECUTOR: signal from {master_wallet} ({side} {symbol} @ {master_price})")

        query = """
                   SELECT 
                       s.id AS subscription_id,
                       s.is_reverse,
                       u.wallet_address AS user_wallet,
                       u.builder_approved,
                       r.volume_per_trade_usd,
                       r.max_total_exposure_usd,
                       r.max_slippage,
                       r.allowed_markets,
                       a.public_key AS agent_pub,
                       a.encrypted_private_key AS agent_priv
                   FROM subscriptions s
                   JOIN users u ON s.user_id = u.id
                   JOIN user_risk_settings r ON s.user_id = r.user_id
                   JOIN agent_wallets a ON s.user_id = a.user_id
                   WHERE s.master_wallet = $1 
                     AND s.is_active = TRUE 
                     AND a.is_active = TRUE;
               """
        try:
            async with self.db_pool.acquire() as conn:
                followers = await conn.fetch(query, master_wallet)
        except Exception as e:
            logger.error(f"❌ Subscriber export error: {e}")
            return

        if not followers:
            logger.info(f"ℹ️ The {master_wallet} wallet has no active subscribers.")
            return

        tasks = []
        for follower in followers:
            user_wallet = follower['user_wallet']
            if user_wallet not in self.subscription_locks:
                self.subscription_locks[user_wallet] = asyncio.Lock()

            tasks.append(self._execute_with_lock(user_wallet, follower, symbol, side, master_price, master_amount,
                                                 master_wallet))
        await asyncio.gather(*tasks)

    async def _execute_with_lock(self, user_wallet, *args):
        """Ensures that signals are sent strictly in sequence for a single subscription."""
        async with self.subscription_locks[user_wallet]:
            await self.execute_user_order(*args)

# ─────────────────────────────────────────────
#EXECUTION — THE LOGIC OF A SINGLE TRANSACTION
# ─────────────────────────────────────────────

    async def execute_user_order(self, follower: dict, symbol: str, side: str,
                                 price: float, master_amount: float, master_wallet: str):
        async with self.rate_limit:

            if follower.get('is_reverse'):
                reverse_map = {
                    "open_long": "open_short", "open_short": "open_long",
                    "close_long": "close_short", "close_short": "close_long"
                }
                side = reverse_map.get(side, side)
                logger.info(f"🔄 REVERSE: The transaction has been reversed to {side}")

            wallet = follower['user_wallet']
            max_limit = float(follower.get('max_total_exposure_usd', 500))
            sub_id = follower['subscription_id']
            is_reduce_only = side.startswith("close")

            trade_record_ids = None
            close_ratio = 1.0

            try:
                real_positions = await pacifica_client.fetch_user_positions(wallet)
                current_exposure = sum(p['value'] for p in real_positions.values())
                market_info = pacifica_client.cache.get('market_info', {}).get(symbol)
                if not market_info:
                    logger.warning(f"⚠️ No market data available {symbol}")
                    return

                allowed_markets = follower.get('allowed_markets')
                if allowed_markets and len(allowed_markets) > 0:
                    if symbol not in allowed_markets and not is_reduce_only:
                        logger.info(f"🚫 {symbol} not on the whitelist for {wallet[:6]}")
                        return

                logger.info(f"🔍 [STEP 1] Retrieving the {wallet[:6]} user's positions via REST...")
                real_positions = await pacifica_client.fetch_user_positions(wallet)
                logger.info(f"📊 The user's positions have been retrieved. A total of {len(real_positions)} instruments are open.")

                if is_reduce_only:
                    logger.info(f"📉 [STEP 2] CLOSE signal {symbol}. Calculating the Ratio...")
                    result = await self._calc_close_amount(
                        sub_id, symbol, master_amount, wallet, real_positions, master_wallet
                    )
                    if result is None:
                        return
                    close_ratio, raw_amount, trade_record_ids = result
                else:
                    raw_amount = await self._calc_open_amount(
                        follower, sub_id, symbol, price, master_amount, real_positions
                    )
                    if raw_amount is None:
                        return

                lot_size = market_info['lot_size']
                steps = math.ceil(raw_amount / lot_size) if is_reduce_only else math.floor(raw_amount / lot_size)
                formatted_size = round(steps * lot_size, 8)

                min_notional = 10.1
                current_val = formatted_size * price
                logger.info(
                    f"🚀 [STEP 3] Final calculation: {side} {formatted_size} {symbol} (~${current_val:.2f}) | Ratio: {close_ratio:.4f}")

                if not is_reduce_only and current_val < min_notional:
                    bumped_steps = math.ceil(min_notional / price / lot_size)
                    bumped_size = round(bumped_steps * lot_size, 8)
                    bumped_val = bumped_size * price

                    if current_exposure + bumped_val > max_limit:
                        logger.warning(f"🚫 Reversal: top-up to the minimum wage (${bumped_val:.2f}) will exceed the user's limit!")
                        return

                    formatted_size = bumped_size
                    current_val = bumped_val
                    logger.info(f"⬆️ Transaction amount < $10. Finalized at {formatted_size} (${formatted_size * price:.2f}$)")

                if formatted_size <= 0:
                    logger.warning(f"⚠️ The lot size is too small ({formatted_size}). Skipping.")
                    return

                amount_str = f"{formatted_size:f}".rstrip('0').rstrip('.')

                await self._sign_and_send(
                    follower, wallet, sub_id, symbol, side,
                    api_side=self._to_api_side(side),
                    amount_str=amount_str, formatted_size=formatted_size,
                    price=price, master_amount=master_amount,
                    is_reduce_only=is_reduce_only,
                    trade_record_ids=trade_record_ids,
                    close_ratio=close_ratio
                )
            except Exception:
                logger.error(f"❌ CRITICAL ERROR for  {wallet[:6]}", exc_info=True)
# ─────────────────────────────────────────────
#AUXILIARY CALCULATION METHODS
# ─────────────────────────────────────────────

    async def _calc_close_amount(self, sub_id: int, symbol: str, master_amount: float,
                                 wallet: str, real_positions: dict, master_wallet: str):
        """Calculates the volume for closing, checking against the actual REST master's position."""

        logger.info(f"📡 REST request for MASTER {master_wallet[:6]} for {symbol}...")
        m_positions = await pacifica_client.fetch_user_positions(master_wallet)
        m_pos = m_positions.get(symbol)

        m_remaining = float(m_pos['amount']) if m_pos else 0.0
        total_m_before = master_amount + m_remaining

        logger.info(f"📈 MASTER data: {master_amount} has been closed; {m_remaining} remains. (Total amount before: {total_m_before})")

        if total_m_before <= 0:
            logger.warning(f"⚠️ Unable to calculate the ratio: total volume of the master <= 0")
            return None

        close_ratio = master_amount / total_m_before

        # ✅ THE 95% RULE = 100% (Self-Healing)
        if close_ratio > 0.9:
            close_ratio = 1.0
            logger.info(f"🎯 Ratio {close_ratio:.4f} > 0.9. 100% forced close (Full Close).")
        else:
            logger.info(f"📊 Calculated Ratio: {close_ratio:.4f} ({close_ratio * 100:.1f}%)")

        u_pos = real_positions.get(symbol)
        if not u_pos:
            logger.warning(f"⚠️ The user does not have an active position {symbol} on the exchange. Synchronizing the database...")
            await self.db_pool.execute(
                "UPDATE copied_trades SET status = 'closed' WHERE subscription_id = $1 AND symbol = $2", sub_id, symbol)
            return None

        raw_amount = u_pos['amount'] * close_ratio
        logger.info(f"📐 The user holds {u_pos['amount']}. At close:  {raw_amount:.8f} (based on Ratio)")

        agg = await self.db_pool.fetchrow(
            "SELECT array_agg(id) as ids FROM copied_trades WHERE subscription_id = $1 AND symbol = $2 AND status = 'open'",
            sub_id, symbol)

        return close_ratio, raw_amount, agg['ids'] if agg and agg['ids'] else []

    async def _calc_open_amount(self, follower: dict, sub_id: int, symbol: str,
                            price: float, master_amount: float, real_positions: dict):

        """
        Calculates the volume for opening/adding to a position.

        Logic:
        - First entry: volume_per_trade_usd / price
        - Additional purchase: proportional to the master position
        - In both cases: capped at max_total_exposure_usd

        Returns raw_amount or None if the trade should be skipped.
        """
        wallet = follower['user_wallet']
        max_limit = float(follower.get('max_total_exposure_usd', 500))
        min_notional = 10.1
        volume_per_trade = float(follower['volume_per_trade_usd'])

        current_exposure = sum(p['value'] for p in real_positions.values())

        if current_exposure >= max_limit:
            logger.warning(f"🚫 LIMIT REACHED: ${current_exposure:.2f} >= ${max_limit:.2f}")
            return None

        available_usd = max_limit - current_exposure
        if available_usd < min_notional:
            logger.warning(
                f"⚠️ The remaining limit (${available_usd:.2f}) is less than the minimum trade size (${min_notional}). Skipping.")
            return None

        real_pos = real_positions.get(symbol)

        if real_pos:
            pos_info = await self.db_pool.fetchrow("""
                    SELECT COALESCE(SUM(master_size), 0) as m_total
                    FROM copied_trades 
                    WHERE subscription_id = $1 AND symbol = $2 AND status = 'open'
                """, sub_id, symbol)
            m_total = float(pos_info['m_total'])
            add_ratio = master_amount / m_total if m_total > 0 else 1.0
            raw_amount = real_pos['amount'] * add_ratio
        else:
            raw_amount = float(follower['volume_per_trade_usd']) / price

        if (raw_amount * price) > available_usd:
            logger.info(f"✂️ The trade is executed from ${raw_amount * price:.2f} down to the remaining limit of ${available_usd:.2f}")
            raw_amount = available_usd / price

        new_value = raw_amount * price
        if current_exposure + new_value > max_limit:
            available = max_limit - current_exposure
            if available <= 0:
                return None
            raw_amount = available / price

        return raw_amount
    def _to_api_side(self, side: str) -> str:
        mapping = {
            "open_long": "bid", "close_long": "ask",
            "open_short": "ask", "close_short": "bid"
        }
        return mapping.get(side, "bid")


    async def _sign_and_send(self, follower, wallet, sub_id, symbol, side, api_side,
                             amount_str, formatted_size, price, master_amount,
                             is_reduce_only, trade_record_ids, close_ratio):
        """Signs the order and sends it via WebSocket. Updates the database based on the result."""

        agent_priv_str = self._decrypt_key(follower['agent_priv'])
        keypair = Keypair.from_base58_string(agent_priv_str)
        timestamp = int(time.time() * 1_000)
        client_order_id = str(uuid.uuid4())

        signature_payload = {
            "symbol": symbol,
            "reduce_only": is_reduce_only,
            "amount": amount_str,
            "side": api_side,
            "slippage_percent": str(follower['max_slippage']),
            "client_order_id": client_order_id,
        }

        if follower.get('builder_approved'):
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
                "agent_wallet": follower['agent_pub'],
                "signature": signature,
                "timestamp": timestamp,
                "expiry_window": 5000,
                **signature_payload
            }},
        }
        max_ws_retries = 3
        response = None

        for attempt in range(max_ws_retries):
            try:
                async with websockets.connect(WS_URL) as websocket:
                    await websocket.send(json.dumps(ws_message))
                    response = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5.0))
                    break
            except Exception as e:
                logger.warning(f"⚠️ WS attempt {attempt + 1}/{max_ws_retries} failed for {wallet[:6]}: {e}")
                if attempt == max_ws_retries - 1:
                    logger.error(f"❌ The WS has finally dropped for {wallet[:6]}. The transaction was not sent.")
                    return
                await asyncio.sleep(0.5)
        if not response: return

        if response.get("code") == 200:
            logger.info(f"✅ Success: {wallet[:6]} {side} {amount_str} {symbol}")
            await self._handle_success(
                is_reduce_only, close_ratio, trade_record_ids,
                sub_id, client_order_id, symbol, api_side, formatted_size, price, master_amount
            )
        else:
            err_msg = response.get('err', '')
            logger.error(f"❌ EXCHANGE ERROR for {wallet[:6]}: {err_msg}")
            if is_reduce_only and trade_record_ids and (
                    response.get("code") == 420 or "No position" in err_msg):
                await self.db_pool.execute(
                    "UPDATE copied_trades SET status = 'closed' WHERE id = ANY($1)",
                    trade_record_ids
                )


    async def _handle_success(self, is_reduce_only, close_ratio, trade_record_ids,
                              sub_id, client_order_id, symbol, api_side,
                              formatted_size, price, master_amount):
        """Updates the database after the order has been successfully executed."""
        if is_reduce_only:
            if close_ratio >= 0.99:
                await self.db_pool.execute(
                    "UPDATE copied_trades SET status = 'closed' WHERE id = ANY($1)",
                    trade_record_ids
                )
            else:
                remain = 1.0 - close_ratio
                await self.db_pool.execute(
                    "UPDATE copied_trades SET size = size * $1, master_size = master_size * $1 WHERE id = ANY($2)",
                    remain, trade_record_ids
                )
        else:
            await self._log_copied_trade(
                sub_id, client_order_id, symbol, api_side,
                formatted_size, price, master_amount
            )

    async def _log_copied_trade(self, sub_id, order_id, symbol, side,
                                size, price, master_size):
        """Saves the newly copied transaction to the database."""
        query = """
                INSERT INTO copied_trades 
                (subscription_id, follower_trade_id, symbol, side, size, entry_price, status, master_size)
                VALUES ($1, $2, $3, $4, $5, $6, 'open', $7)
            """
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(query, sub_id, order_id, symbol, side, size, price, master_size)
        except Exception as e:
            logger.error(f"❌ Failed to log the transaction: {e}")

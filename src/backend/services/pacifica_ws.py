import asyncio
import websockets
import json
import logging
import os
from collections import deque
from db.liquidations import insert_liquidation

logger = logging.getLogger(__name__)
class PacificaWSConnection:
    def __init__(self, ws_uri, headers, conn_id, listener):
        self.ws_uri = ws_uri
        self.headers = headers
        self.conn_id = conn_id
        self.listener = listener
        self.ws = None
        self.is_running = False
        self.active_subs = {}

        self._connected = asyncio.Event()

    async def wait_until_connected(self, timeout=10):
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ WS [{self.conn_id}] connection timeout after {timeout}s")

    async def start(self):
        self.is_running = True
        while self.is_running:
            try:
                async with websockets.connect(
                    self.ws_uri,
                    additional_headers=self.headers,
                    ping_interval=None
                ) as ws:
                    self.ws = ws

                    self._connected.set()
                    logger.info(f"✅ WS Connection [{self.conn_id}] established.")

                    for sub_id, params in self.active_subs.items():
                        await self.send_subscribe(params)
                        logger.info(f"🔄 [{self.conn_id}] Re-subscribed: {sub_id}")

                    ping_task = asyncio.create_task(self._keep_alive())
                    try:
                        await self._listen_loop()
                    finally:
                        ping_task.cancel()

            except Exception as e:
                if self.is_running:
                    logger.warning(f"⚠️ WS [{self.conn_id}] reconnecting in 5s... Error: {e}")

                self._connected.clear()
                self.ws = None
                await asyncio.sleep(5)

    async def _listen_loop(self):
        async for message in self.ws:
            if not self.is_running:
                break
            await self.listener.process_message(message)

    async def _keep_alive(self):
        try:
            while self.is_running:
                await asyncio.sleep(30)
                if self.ws:
                    await self.ws.send(json.dumps({"method": "ping"}))
        except Exception:
            pass

    async def send_subscribe(self, params):
        if self.ws:
            await self.ws.send(json.dumps({"method": "subscribe", "params": params}))

    async def send_unsubscribe(self, params):
        if self.ws:
            await self.ws.send(json.dumps({"method": "unsubscribe", "params": params}))

    def stop(self):
        self.is_running = False
        self._connected.clear()
        if self.ws:
            asyncio.create_task(self.ws.close())


class PacificaWSListener:
    def __init__(self, ws_uri: str, ws_manager, db_pool=None, executor=None):
        self.ws_uri = ws_uri
        self.ws_manager = ws_manager
        self.db_pool = db_pool
        self.executor = executor

        self.ticker = []
        self._is_running = False
        self.processed_txs = deque(maxlen=5000)
        self.LIQUIDATION_CAUSES = {"market_liquidation", "backstop_liquidation"}
        self.HOT_TOKENS = ['BTC', 'ETH', 'SOL']

        self.connections = []
        self.global_sub_counts = {}
        self.extra_headers = {}
        self.MAX_SUBS = 18

        self._tasks = []

        self.active_masters = set()

    def set_markets(self, ticker: list[str]):
        self.ticker = ticker

    def _generate_sub_id(self, params: dict) -> str:
        parts = [f"{k}={v}" for k, v in sorted(params.items())]
        return "_".join(parts)

    # FIX 4: Хелпер для создания соединения с сохранением task
    def _create_connection(self, conn_id: int) -> PacificaWSConnection:
        conn = PacificaWSConnection(self.ws_uri, self.extra_headers, conn_id, self)
        self.connections.append(conn)
        task = asyncio.create_task(conn.start())
        self._tasks.append(task)
        return conn

    async def start(self):
        self._is_running = True

        api_key = os.getenv("PACIFICA_API_KEY")
        self.extra_headers = {"PF-API-KEY": api_key} if api_key else {}

        if not self.ticker:
            logger.error("❌ No markets for WS!")
            return

        num_conns = max(4, (len(self.ticker) // self.MAX_SUBS) + 1)

        for i in range(num_conns):
            self._create_connection(i)

        logger.info(f"🚀 Initialized WS Pool with {num_conns} connections.")

        logger.info("⏳ Waiting for all connections to establish...")
        await asyncio.gather(*[conn.wait_until_connected() for conn in self.connections])
        logger.info("✅ All connections ready. Subscribing...")

        for ticker in self.ticker:
            await self.subscribe({"source": "trades", "symbol": ticker})
        for hot_token in self.HOT_TOKENS:
            if hot_token in self.ticker:
                await self.subscribe({"source": "book", "symbol": hot_token, "agg_level": 1})

        self._tasks.append(asyncio.create_task(self._sync_masters_loop()))
        logger.info("🕵️‍♂️ Master sync loop started.")
        logger.info(f"📡 Distributed {len(self.ticker)} trade subs + {len(self.HOT_TOKENS)} book subs across the pool.")

    async def add_master_instantly(self, master_address: str):
        if master_address not in self.active_masters:
            await self.subscribe({"source": "account_trades", "account": master_address})
            self.active_masters.add(master_address)
            logger.info(f"⚡ МОМЕНТАЛЬНАЯ подписка на мастера: {master_address}")

    async def remove_master_instantly(self, master_address: str):
        if master_address not in self.active_masters:
            return

        try:
            async with self.db_pool.acquire() as conn:
                active_followers_count = await conn.fetchval(
                    """
                    SELECT COUNT(*) 
                    FROM subscriptions 
                    WHERE master_wallet = $1 AND is_active = TRUE
                    """,
                    master_address
                )

            if active_followers_count == 0:
                sub_trades = self._generate_sub_id({"source": "account_trades", "account": master_address})

                await self.unsubscribe(sub_trades)

                self.active_masters.remove(master_address)

                logger.info(f"🗑️ WebSocket связь разорвана: на мастера {master_address} больше нет подписчиков.")
            else:
                logger.info(
                    f"ℹ️ Связь сохранена: за мастером {master_address} всё еще следят {active_followers_count} чел.")

        except Exception as e:
            logger.error(f"Ошибка при попытке отписки от мастера {master_address}: {e}")

    async def _sync_masters_loop(self):
        while self._is_running:
            if not self.db_pool:
                await asyncio.sleep(10)
                continue

            try:
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch("SELECT DISTINCT master_wallet FROM subscriptions WHERE is_active = TRUE")
                    #TODO
                    db_masters = set(row['master_wallet'] for row in rows)

                to_add = db_masters - self.active_masters
                for master in to_add:
                    await self.subscribe({"source": "account_trades", "account": master})
                    self.active_masters.add(master)
                    logger.info(f"🎯 Начали следить за новым мастером: {master}")

                to_remove = self.active_masters - db_masters
                for master in to_remove:
                    sub_trades = self._generate_sub_id({"source": "account_trades", "account": master})
                    await self.unsubscribe(sub_trades)
                    self.active_masters.remove(master)
                    logger.info(f"🛑 Прекратили следить за мастером: {master}")
            except Exception as e:
                logger.error(f"Ошибка синхронизации мастеров: {e}")
            await asyncio.sleep(20)

    def stop(self):
        self._is_running = False
        for conn in self.connections:
            conn.stop()

        for task in self._tasks:
            task.cancel()
        logger.info("🛑 WS Pool stopped")

    async def subscribe(self, params: dict) -> str:
        sub_id = self._generate_sub_id(params)

        if sub_id in self.global_sub_counts:
            self.global_sub_counts[sub_id]["count"] += 1
            return sub_id

        best_conn = min(self.connections, key=lambda c: len(c.active_subs))

        if len(best_conn.active_subs) >= self.MAX_SUBS:
            new_id = len(self.connections)
            best_conn = self._create_connection(new_id)
            await best_conn.wait_until_connected()
            logger.info(f"🔄 Scaled WS Pool: Added Connection [{new_id}]")

        self.global_sub_counts[sub_id] = {
            "count": 1,
            "params": params,
            "conn_id": best_conn.conn_id,
        }
        best_conn.active_subs[sub_id] = params
        await best_conn.send_subscribe(params)
        return sub_id

    async def unsubscribe(self, sub_id: str):
        is_hot = any(f"symbol={ht}" in sub_id for ht in self.HOT_TOKENS)
        is_trade = "source=trades" in sub_id
        if is_hot or is_trade:
            return

        if sub_id not in self.global_sub_counts:
            return

        self.global_sub_counts[sub_id]["count"] -= 1

        if self.global_sub_counts[sub_id]["count"] <= 0:
            info = self.global_sub_counts.pop(sub_id)
            params = info["params"]

            conn = next((c for c in self.connections if c.conn_id == info["conn_id"]), None)
            if conn:
                conn.active_subs.pop(sub_id, None)
                await conn.send_unsubscribe(params)
                logger.info(f"🔕 [{conn.conn_id}] Unsubscribed: {sub_id}")

    async def process_message(self, message: str):
        try:
            data = json.loads(message)

            if "rl" in data:
                remaining = data["rl"].get("r", 0) / 10
                if remaining < 100:
                    logger.warning(f"⚠️ LOW RATE LIMIT (WS): {remaining} credits left.")

            channel = data.get("channel")

            if not channel or channel == "pong":
                return

            if channel == "trades":
                liquidations = [
                    t for t in data.get("data", [])
                    if t.get("tc") in self.LIQUIDATION_CAUSES
                ]
                if liquidations:
                    await self._handle_liquidations(liquidations)

            elif channel == "book":
                if "data" in data:
                    payload = {
                        "type": "orderbook_update",
                        "symbol": data["data"].get("s"),
                        "timestamp": data["data"].get("t"),
                        "bids": data["data"]["l"][0],
                        "asks": data["data"]["l"][1],
                    }
                    await self.ws_manager.broadcast(payload)

            elif channel == "account_trades":
                if "data" in data and data["data"]:
                    await self._handle_master_trades(data["data"])

            elif channel not in ("trades", "book", "account_trades"):
                await self.ws_manager.broadcast(data)

        except Exception as e:
            logger.error(f"Error processing WS message: {e}", exc_info=True)

    async def _handle_master_trades(self, trades_list: list):
        for trade in trades_list:
            master_address = trade.get("u")
            symbol = trade.get("s")
            side = trade.get("ts")
            price = float(trade.get("p", 0))
            master_amount = float(trade.get("a", 0))

            logger.info(f"🚨 СИГНАЛ: Мастер {master_address} сделал {side} по {symbol} за ${price}!")

            if self.executor:
                asyncio.create_task(
                    self.executor.process_master_signal(master_address, symbol, side, price, master_amount)
                )
            else:
                logger.error("❌ Экзекутор не подключен к Листенерам!")

    async def _handle_liquidations(self, liquidations: list):
        batch_for_front = []

        for trade in liquidations:
            trade_id = trade.get("h")
            if trade_id in self.processed_txs:
                continue
            if trade_id:
                self.processed_txs.append(trade_id)

            clean_obj = {
                "trade_id": trade_id,
                "coin": trade.get("s"),
                "price": float(trade.get("p", 0)),
                "size": float(trade.get("a", 0)),
                "usd_amount": float(trade.get("p", 0)) * float(trade.get("a", 0)),
                "side": trade.get("d"),
                "liq_type": trade.get("tc"),
                "timestamp": trade.get("t"),
                "nonce": trade.get("li"),
            }
            batch_for_front.append(clean_obj)

            if self.db_pool:
                asyncio.create_task(insert_liquidation(self.db_pool, clean_obj))

        if batch_for_front:
            coins = list(set(obj["coin"] for obj in batch_for_front))
            logger.info(f"🔥 Got {len(batch_for_front)} liquidations {coins}!")
            await self.ws_manager.broadcast({
                "type": "liquidations",
                "data": batch_for_front,
            })
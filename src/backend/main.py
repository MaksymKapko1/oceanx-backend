import asyncio
import logging
import json
import time

import uvicorn
import asyncpg
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocket
from dotenv import load_dotenv
from services.trade_executor import CopyTradeExecutor

load_dotenv()

from services.pacifica_client import pacifica_client
from routers.pnl_leaderboard import router as leaderboard_router
from routers.liquidations_router import router as liquidations_router
from routers.overview import router as stats_router
from routers.auth_router import router as auth_router
from routers import copy_router
from routers.user_settings_router import router as settings_router
from routers.manual_trades_router import router as manual_trades_router
from db.schema import init_db
from core.connection_manager import ws_manager
from services.pacifica_ws import PacificaWSListener
from core.dependencies import verify_privy_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    user = os.getenv("DB_USER", 'maksym')
    password = os.getenv("DB_PASS", '')
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "maksym")

    DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

print(f"👉 CONNECTING TO: {DATABASE_URL}")
WS_URL = "wss://ws.pacifica.fi/ws"

ws_listener = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global ws_listener

    db_pool = await asyncpg.create_pool(DATABASE_URL)
    app.state.db_pool = db_pool
    await init_db(db_pool)

    executor = CopyTradeExecutor(db_pool)
    app.state.executor = executor

    ws_listener = PacificaWSListener(
        ws_uri=WS_URL,
        ws_manager=ws_manager,
        db_pool=db_pool,
        executor=executor
    )

    app.state.ws_listener = ws_listener

    client_task = asyncio.create_task(pacifica_client.run(db_pool))

    retries = 0
    while not pacifica_client.cache.get('markets') and retries < 10:
        await asyncio.sleep(0.5)
        retries += 1

    markets_list = pacifica_client.cache.get('markets', [])

    if not markets_list:
        logger.error("❌ The markets didn't load at startup!")

    ws_listener.set_markets(markets_list)
    listener_task = asyncio.create_task(ws_listener.start())

    logger.info("🚀 The server is running")
    yield

    ws_listener.stop()
    await pacifica_client.stop()
    client_task.cancel()
    listener_task.cancel()
    await db_pool.close()
    logger.info("🛑 The server has been stopped")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173",
                   "https://oceanxx.xyz",
                   "https://www.oceanxx.xyz",
                   "https://oceanx-frontend.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(leaderboard_router)
app.include_router(liquidations_router)
app.include_router(stats_router)
app.include_router(auth_router)
app.include_router(copy_router.router)
app.include_router(settings_router)
app.include_router(manual_trades_router)

@app.get("/")
async def root():
    return {"message": "Pacifica CopyTrade API"}

@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        auth_msg_raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        auth_data = json.loads(auth_msg_raw)

        if auth_data.get('action') != 'auth':
            await websocket.close(code=4001, reason="Auth required as first message")
            return

        token = auth_data.get('token')
        if token and token != "null":
            try:
                user_info = verify_privy_token(token)
                if not user_info:
                    await websocket.close(code=4003, reason="Invalid token")
                    return
            except HTTPException:
                await websocket.close(code=4003, reason="Token verification failed")
                return
            except Exception as e:
                logger.error(f"WS Auth Error: {e}")
                await websocket.close(code=4003, reason="Token error")
                return
    except asyncio.TimeoutError:
        await websocket.close(code=4001, reason="Auth timeout")
        return
    except Exception as e:
        await websocket.close(code=4000, reason="Invalid format")
        return

    await ws_manager.connect(websocket)

    MAX_MESSAGES_PER_MINUTE = 60
    message_count = 0
    start_time = time.time()

    user_subscriptions = set()

    try:
        while True:
            data = await websocket.receive_text()

            current_time = time.time()
            if current_time - start_time > 60:
                start_time = current_time
                message_count = 0
            message_count += 1
            if message_count > MAX_MESSAGES_PER_MINUTE:
                await websocket.close(code=4029, reason="Rate limit exceeded")
                break

            msg = json.loads(data)
            action = msg.get("action")
            params = msg.get("params")

            if not params or not isinstance(params, dict):
                continue

            if action == "subscribe":
                if ws_listener:
                    sub_id = await ws_listener.subscribe(params)
                    user_subscriptions.add(sub_id)

            elif action == "unsubscribe":
                if ws_listener:
                    sub_id = ws_listener._generate_sub_id(params)
                    if sub_id in user_subscriptions:
                        user_subscriptions.remove(sub_id)
                        await ws_listener.unsubscribe(sub_id)

    except json.JSONDecodeError:
        logger.warning("Invalid JSON received from the client")

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)

        if ws_listener:
            for sub_id in user_subscriptions:
                asyncio.create_task(ws_listener.unsubscribe(sub_id))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", reload=True, port=8000)
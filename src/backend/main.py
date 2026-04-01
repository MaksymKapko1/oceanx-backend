import asyncio
import logging
import json
import uvicorn
import asyncpg
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocket
from dotenv import load_dotenv
load_dotenv()

from services.pacifica_client import pacifica_client
from routers.pnl_leaderboard import router as leaderboard_router
from routers.liquidations_router import router as liquidations_router
from routers.overview import router as stats_router
from db.schema import init_db
from core.connection_manager import ws_manager
from services.pacifica_ws import PacificaWSListener


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

WS_URL = "wss://ws.pacifica.fi/ws"

ws_listener = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ws_listener

    db_pool = await asyncpg.create_pool(DATABASE_URL)
    app.state.db_pool = db_pool
    await init_db(db_pool)

    ws_listener = PacificaWSListener(
        ws_uri=WS_URL,
        ws_manager=ws_manager,
        db_pool=db_pool
    )

    client_task = asyncio.create_task(pacifica_client.run(db_pool))

    retries = 0
    while not pacifica_client.cache.get('markets') and retries < 10:
        await asyncio.sleep(0.5)
        retries += 1

    markets_list = pacifica_client.cache.get('markets', [])

    if not markets_list:
        logger.error("❌ Рынки не загрузились при старте!")

    ws_listener.set_markets(markets_list)
    listener_task = asyncio.create_task(ws_listener.start())

    logger.info("🚀 Сервер запущен")
    yield

    # --- ВЫКЛЮЧЕНИЕ СЕРВЕРА ---
    ws_listener.stop()
    await pacifica_client.stop()
    client_task.cancel()
    listener_task.cancel()
    await db_pool.close()
    logger.info("🛑 Сервер остановлен")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(leaderboard_router)
app.include_router(liquidations_router)
app.include_router(stats_router)


@app.get("/")
async def root():
    return {"message": "Pacifica CopyTrade API"}


@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)

    user_subscriptions = set()

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")
                params = msg.get("params")

                if not params:
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
                logger.warning("Получен невалидный JSON от клиента")

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)

        if ws_listener:
            for sub_id in user_subscriptions:
                asyncio.create_task(ws_listener.unsubscribe(sub_id))

if __name__ == "__main__":
    uvicorn.run("main:app", reload=True, port=8001)
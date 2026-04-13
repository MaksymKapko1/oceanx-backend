import asyncio
import websockets
import json
import logging
import os
import time

from services.pacifica_client import pacifica_client

logger = logging.getLogger(__name__)

# УМНЫЙ КЭШ ПОЗИЦИЙ
# Структура: {"address": {"timestamp": 12345678, "data": [список позиций]}}
POSITIONS_CACHE = {}
CACHE_TTL = 60  # Скачиваем позиции с биржи не чаще 1 раза в 60 секунд!


async def fetch_raw_positions_from_ws(account_address: str) -> list:
    """Функция, которая реально ходит в WebSocket (тяжелая)"""
    ws_uri = "wss://ws.pacifica.fi/ws"
    api_key = os.getenv("PACIFICA_API_KEY")
    headers = {"PF-API-KEY": api_key} if api_key else {}

    subscribe_msg = {
        "method": "subscribe",
        "params": {"source": "account_positions", "account": account_address}
    }

    try:
        async with websockets.connect(ws_uri, additional_headers=headers) as websocket:
            await websocket.send(json.dumps(subscribe_msg))
            for _ in range(30):
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                data = json.loads(response)
                if data.get('channel') == 'account_positions':
                    return data.get('data', [])
    except asyncio.TimeoutError:
        logger.warning(f"⏳ Таймаут получения позиций (WS) для {account_address}")
    except Exception as e:
        logger.error(f"❌ Ошибка WS снапшота для {account_address}: {e}")

    return []


async def fetch_acc_positions(account_address: str) -> list:
    """Умная функция-обертка: отдает данные из кэша и пересчитывает PNL"""
    current_time = time.time()

    # 1. ПРОВЕРЯЕМ КЭШ
    # Если позиций нет в кэше, или они старше CACHE_TTL (60 сек) - идем в WebSocket
    if account_address not in POSITIONS_CACHE or (
            current_time - POSITIONS_CACHE[account_address]['timestamp']) > CACHE_TTL:
        raw_positions = await fetch_raw_positions_from_ws(account_address)

        # Сохраняем сырые позиции (без PNL) в кэш
        if raw_positions:  # Кэшируем только если биржа реально что-то отдала
            POSITIONS_CACHE[account_address] = {
                "timestamp": current_time,
                "data": raw_positions
            }
        else:
            # Если биржа вернула пустоту или ошибку, но в кэше есть старые данные, берем их
            raw_positions = POSITIONS_CACHE.get(account_address, {}).get("data", [])
    else:
        # Иначе просто берем из кэша!
        raw_positions = POSITIONS_CACHE[account_address]['data']

    # 2. ПЕРЕСЧИТЫВАЕМ PNL В РЕАЛЬНОМ ВРЕМЕНИ
    # Независимо от того, откуда мы взяли сырые позиции, PNL мы считаем по самым СВЕЖИМ ценам!
    markets_data = pacifica_client.cache.get('top_volume', [])
    mark_prices = {m['symbol']: m['mark_price'] for m in markets_data}

    enriched_positions = []
    for pos in raw_positions:
        # Важно: делаем копию словаря, чтобы не мутировать кэш
        pos_copy = dict(pos)

        sym = pos_copy.get('s')
        mark_price = mark_prices.get(sym)
        entry_price = float(pos_copy.get('p', 0))
        size = float(pos_copy.get('a', 0))

        pnl = 0
        if mark_price and entry_price > 0:
            if pos_copy.get('d') == 'bid':  # LONG
                pnl = (mark_price - entry_price) * size
            else:  # SHORT
                pnl = (entry_price - mark_price) * size

        pos_copy['calculated_pnl'] = pnl
        enriched_positions.append(pos_copy)

    return enriched_positions
import asyncio
import websockets
import json


async def simulate_hacker():
    uri = "ws://localhost:8000/ws"
    try:
        async with websockets.connect(uri) as websocket:
            # 1. Авторизуемся как гость
            await websocket.send(json.dumps({"action": "auth", "token": None}))
            print("🚀 Авторизация отправлена. Начинаем спам...")

            for i in range(1, 101):
                # Отправляем подписку
                msg = {"action": "subscribe", "params": {"symbol": "BTC", "id": i}}
                await websocket.send(json.dumps(msg))

                # Добавляем маленькую задержку, чтобы сервер успевал отвечать
                await asyncio.sleep(0.01)

                if i % 10 == 0:
                    print(f"Отправлено {i} сообщений...")

    except websockets.exceptions.ConnectionClosedError as e:
        print(f"\n🛡️ СЕРВЕР ЗАКРЫЛ СОЕДИНЕНИЕ!")
        print(f"Код ошибки: {e.code}")
        print(f"Причина: {e.reason}")
    except Exception as e:
        print(f"Ошибка: {e}")


asyncio.run(simulate_hacker())
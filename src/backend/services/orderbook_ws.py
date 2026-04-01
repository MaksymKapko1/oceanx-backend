import asyncio
import json
import logging
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

class OrderBookWS:
    def __init__(self, ws_uri: str, ws_manager):
        self.ws_uri = ws_uri
        self.ws_manager = ws_manager
        self._is_running = False
        self._ws = None
        self._current_symbol = None
        self._current_agg = 1
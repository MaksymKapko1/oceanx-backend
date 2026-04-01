import aiohttp
import asyncio
import logging
import asyncpg
import time
import os
from typing import Optional

from db.volume import insert_daily_volume

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

BASE_URL = 'https://api.pacifica.fi/api/v1/'

class PacificaClient:
    def __init__(self, base_url = "https://api.pacifica.fi"):
        self.base_url = base_url
        self.api_key = os.getenv("PACIFICA_API_KEY")
        self.session: Optional[aiohttp.ClientSession] = None
        self.pool: Optional[asyncpg.Pool] = None
        self.cache = {
            'pnl_1d_leaderboard': [],
            'markets': {},
            'daily_volume': 0.0,
            'open_interest': 0.0,
            'top_volume': [],
            'top_oi': [],
            'mark_price': 0.0,
        }
        self.refresh_interval = 10
        self.scheduler = AsyncIOScheduler(timezone="UTC")

    def _get_headers(self):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if self.api_key:
            headers["PF-API-KEY"] = self.api_key
        return headers

    async def start(self):
        self.session = aiohttp.ClientSession(
            base_url=BASE_URL,
            timeout=aiohttp.ClientTimeout(total=self.refresh_interval)
        )
        if self.api_key:
            logger.info(f"🔑 PacificaClient initialized with API Key (starts with: {self.api_key[:8]}...)")
        else:
            logger.warning("⚠️ PacificaClient: No API Key found. Using strict rate limits!")
    async def stop(self):
        if self.session:
            await self.session.close()

    async def run(self, pool: asyncpg.Pool):
        self.pool = pool
        await self.start()
        await self.fetch_markets()

        await self.fetch_daily_volume()
        await self.fetch_pnl_leaderboard()

        asyncio.create_task(self.fetch_historical_volume_full())

        self.scheduler.add_job(
            self.fetch_historical_volume_incremental(),
            CronTrigger(hour=0, minute=5),
            id="daily_volume_sync"
        )
        self.scheduler.start()
        logger.info("⏰ Планировщик запущен: обновление объемов каждый день в 00:05 UTC")

        while True:
            await self.fetch_pnl_leaderboard()
            await self.fetch_daily_volume()
            await asyncio.sleep(self.refresh_interval)

    async def _fetch(self, endpoint: str) -> Optional[dict | list]:
        try:
            async with self.session.get(endpoint, headers=self._get_headers()) as response:
                rl_header = response.headers.get("ratelimit")
                if rl_header:
                    parts = {p.split('=')[0]: p.split('=')[1] for p in rl_header.split(';') if '=' in p}
                    remaining = float(parts.get('r', 0)) / 10  # Делим на 10, как просят в доках
                    refresh_in = parts.get('t', 0)

                    if remaining < 50:
                        logger.warning(
                            f"⚠️ LOW RATE LIMIT (REST): {remaining} credits left. Refreshes in {refresh_in}s")
                if response.status == 200:
                    return await response.json()
                logger.warning(f"⚠️ {endpoint} → HTTP {response.status}")
                return None
        except aiohttp.ClientConnectionError:
            logger.error(f"❌ Нет соединения: {endpoint}")
        except aiohttp.ServerTimeoutError:
            logger.error(f"⏳ Таймаут: {endpoint}")
        except Exception as e:
            logger.error(f"❌ Fetch error {endpoint}: {e}")
        return None

    async def fetch_pnl_leaderboard(self, period: str = '1d') -> list:
        """"https://api.pacifica.fi/api/v1/leaderboard?limit=25000"""
        data = await self._fetch('leaderboard?limit=25000')

        if not data:
            return self.cache['pnl_1d_leaderboard']

        raw_leaderboard = data.get('data', [])
        if not raw_leaderboard:
            return self.cache['pnl_1d_leaderboard']

        sort_field = f'pnl_{period}' if period != 'all_time' else 'pnl_all_time'

        sorted_leaderboard = sorted(
            raw_leaderboard,
            key=lambda x: float(x.get(sort_field) or 0),
            reverse=True
        )

        self.cache['pnl_1d_leaderboard'] = sorted_leaderboard
        # logger.info(f"✅ Leaderboard загружен: {len(sorted_leaderboard)} трейдеров, сортировка по {sort_field}")
        return sorted_leaderboard

    async def fetch_markets(self) -> list[str]:
        """"info"""
        data = await self._fetch('info')

        if not data:
            return self.cache.get('markets', [])

        raw_markets = data.get('data', [])
        ticker = [market['symbol'] for market in raw_markets]
        self.cache['markets'] = ticker

        logger.info(f"✅ Tickers loaded: {len(ticker)} → {ticker}")

        return ticker

    async def fetch_daily_volume(self) -> tuple[float, float]:
        """"info/prices"""
        data = await self._fetch('info/prices')

        if not data:
            return self.cache.get('daily_volume', 0), self.cache.get('open_interest', 0)

        coins_data = data.get('data', [])
        if not coins_data:
            return self.cache.get('daily_volume', 0), self.cache.get('open_interest', 0)

        total_volume = 0
        total_open_interest_usd = 0

        market_stats = []

        for coin in coins_data:
            symbol = coin.get('symbol', 'UNKNOWN COIN')
            vol_str = coin.get('volume_24h', '0')
            oi_str = coin.get('open_interest', '0')
            mark_price = float(coin.get('mark', '0') or 0)

            vol_float = float(vol_str)
            oi_tokens = float(oi_str)
            oi_usd = oi_tokens * mark_price

            total_open_interest_usd += oi_usd
            total_volume += vol_float

            market_stats.append({
                'symbol': symbol,
                'volume_24h': vol_float,
                'open_interest': oi_usd,
                'oi_tokens': oi_tokens,
                'mark_price': mark_price
            })

        top_volume = sorted(market_stats, key=lambda x: x['volume_24h'], reverse=True)
        top_oi = sorted(market_stats, key=lambda x: x['open_interest'], reverse=True)

        self.cache['daily_volume'] = total_volume
        self.cache['open_interest'] = total_open_interest_usd
        self.cache['top_volume'] = top_volume
        self.cache['top_oi'] = top_oi

        return total_volume, total_open_interest_usd

    async def fetch_historical_volume_full(self):
        logger.info("🔄 Запуск полного исторического сбора объемов...")

        markets = self.cache.get('markets', [])
        if not markets:
            markets = await self.fetch_markets()

        # Начало истории Pacifica
        start_time = 1749427200000
        end_time = int(time.time() * 1000)

        await self._collect_and_save_volume(markets, start_time, end_time)
        logger.info("✅ Полный исторический сбор завершён")

    async def fetch_historical_volume_incremental(self):
        """Инкрементальный сбор: только последние 2 дня (с запасом на таймзоны).
        Запускается ежедневно в 00:05 UTC."""
        logger.info("📅 Инкрементальный сбор: последние 2 дня...")

        markets = self.cache.get('markets', [])
        if not markets:
            markets = await self.fetch_markets()

        # Берём 2 дня назад с запасом, чтобы не пропустить свечу
        two_days_ago = int(time.time() * 1000) - (2 * 24 * 60 * 60 * 1000)
        end_time = int(time.time() * 1000)

        await self._collect_and_save_volume(markets, two_days_ago, end_time)
        logger.info("✅ Инкрементальный сбор завершён")

    async def _collect_and_save_volume(self, markets: list, start_time: int, end_time: int):
        """Общая логика: собрать свечи по всем тикерам и записать в БД."""
        daily_totals = {}

        for ticker in markets:
            response = await self._fetch(
                f"kline?symbol={ticker}&interval=1d&start_time={start_time}&end_time={end_time}"
            )
            if not response or 'data' not in response:
                logger.warning(f"⚠️ Нет данных для {ticker}, пропускаем")
                continue

            for candle in response.get('data', []):
                t = candle['t']
                v = float(candle['v'])
                c = float(candle['c'])
                volume_usd = (v * c)

                daily_totals[t] = daily_totals.get(t, 0) + volume_usd

            logger.info(f"📈 Обработан {ticker}")
            await asyncio.sleep(4)  # rate limit

        if not self.pool:
            logger.error("❌ pool не задан, запись в БД невозможна")
            return

        for t, vol in daily_totals.items():
            await insert_daily_volume(self.pool, t, vol)

        logger.info(f"💾 Записано {len(daily_totals)} дней в БД")

pacifica_client = PacificaClient()
import aiohttp
import asyncio
import logging
import asyncpg
import time
import os
import itertools
from typing import Optional

from db.volume import insert_daily_volume

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

BASE_URL = 'https://api.pacifica.fi/api/v1/'

class PacificaClient:
    def __init__(self, base_url = "https://api.pacifica.fi"):
        self.base_url = base_url
        self.api_key = os.getenv("PACIFICA_API_KEYS")
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
        raw_keys = os.getenv("PACIFICA_API_KEYS")
        self.api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

        if self.api_keys:
            self.key_rotator = itertools.cycle(self.api_keys)
            logger.info(f"🔑 The rotator is good: {len(self.api_keys)} keys in the pool.")
        else:
            self.key_rotator = None
            logger.warning("⚠️ PACIFICA_API_KEYS not found! Operating without keys (strict limits).")
        self.scheduler = AsyncIOScheduler(timezone="UTC")

    def _get_headers(self):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if self.key_rotator:
            current_key = next(self.key_rotator)
            headers["PF-API-KEY"] = current_key

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
            self.fetch_historical_volume_incremental,
            CronTrigger(hour=0, minute=5),
            id="daily_volume_sync"
        )
        self.scheduler.start()
        logger.info("⏰ The scheduler is running: volumes are updated daily at 00:05 UTC")

        while True:
            try:
                await self.fetch_pnl_leaderboard()
                await self.fetch_daily_volume()
            except Exception as e:
                logger.error(f"🔥 ERROR IN THE BACKGROUND LOOP (but life goes on): {e}", exc_info=True)
            await asyncio.sleep(self.refresh_interval)

    async def _fetch(self, endpoint: str) -> dict | None | int:
        if self.session is None or self.session.closed:
            logger.warning("⚠️ The session has been closed; let's recreate it...")
            await self.start()

        try:
            async with self.session.get(endpoint, headers=self._get_headers()) as response:
                rl_header = response.headers.get("ratelimit")
                refresh_in = 0
                if rl_header:
                    parts = {p.split('=')[0]: p.split('=')[1] for p in rl_header.split(';') if '=' in p}
                    remaining = float(parts.get('r', 0)) / 10
                    refresh_in = int(float(parts.get('t', 0)))

                    if remaining < 50:
                        logger.warning(
                            f"⚠️ LOW RATE LIMIT (REST): {remaining} credits left. Refreshes in {refresh_in}s")
                if response.status == 200:
                    return await response.json()

                if response.status == 429:
                    wait_time = refresh_in if refresh_in > 0 else 60
                    logger.warning(f"🛑 RATE LIMIT HIT! The exchange asks to wait {wait_time} seconds.")
                    return wait_time
                logger.warning(f"⚠️ {endpoint} → HTTP {response.status}")
                return None
        except aiohttp.ClientConnectionError:
            logger.error(f"❌ No connection: {endpoint}")
        except aiohttp.ServerTimeoutError:
            logger.error(f"⏳ Timeout: {endpoint}")
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

        clean_data = [
            x for x in raw_leaderboard
            if float(x.get('volume_all_time') or 0) > 0
        ]
        clean_data.sort(key=lambda x: float(x.get('pnl_1d') or 0), reverse=True)

        self.cache['pnl_1d_leaderboard'] = clean_data
        # logger.info(f"✅ Leaderboard is loaded: {len(sorted_leaderboard)} traders, sort by {sort_field}")
        return clean_data

    async def fetch_markets(self) -> list[str]:
        """"info"""
        data = await self._fetch('info')

        if not data:
            return self.cache.get('markets', [])

        raw_markets = data.get('data', [])
        market_info_dict = {}
        tickers = []

        for market in raw_markets:
            symbol = market['symbol']
            tickers.append(symbol)

            market_info_dict[symbol] = {
                'lot_size': float(market['lot_size']),
                'tick_size': float(market['tick_size']),
                'max_leverage': float(market.get('max_leverage', 1.0))
            }
        self.cache['markets'] = tickers
        self.cache['market_info'] = market_info_dict

        logger.info(f"✅ Tickers loaded: {len(tickers)} → {tickers}")

        return tickers

    async def fetch_daily_volume(self) -> tuple[float, float]:
        """"info/prices"""
        endpoint = f'info/prices?t={int(time.time() * 1000)}'
        data = await self._fetch(endpoint)

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
        logger.info("🔄 Launching a full historical volume collection...")

        markets = self.cache.get('markets', [])
        if not markets:
            markets = await self.fetch_markets()
        start_time = 1749427200000
        end_time = int(time.time() * 1000)

        await self._collect_and_save_volume(markets, start_time, end_time)
        logger.info("✅ The complete historical collection is now complete")

    async def fetch_historical_volume_incremental(self):
        """Incremental collection: only the last 2 days (with a buffer for time zones).
        Runs daily at 00:05 UTC."""
        logger.info("📅 Incremental fundraising: the last 2 days...")

        markets = self.cache.get('markets', [])
        if not markets:
            markets = await self.fetch_markets()

        two_days_ago = int(time.time() * 1000) - (2 * 24 * 60 * 60 * 1000)
        end_time = int(time.time() * 1000)

        await self._collect_and_save_volume(markets, two_days_ago, end_time)
        logger.info("✅ The incremental collection is complete")

    async def _collect_and_save_volume(self, markets: list, start_time: int, end_time: int):
        """General approach: Collect candlestick data for all tickers and store it in the database."""
        daily_totals = {}

        for ticker in markets:
            response = await self._fetch(
                f"kline?symbol={ticker}&interval=1d&start_time={start_time}&end_time={end_time}"
            )
            if not response or 'data' not in response:
                logger.warning(f"⚠️ No data available for {ticker}; skipping")
                continue

            for candle in response.get('data', []):
                t = candle['t']
                v = float(candle['v'])
                c = float(candle['c'])
                volume_usd = (v * c)

                daily_totals[t] = daily_totals.get(t, 0) + volume_usd

            await asyncio.sleep(4)

        if not self.pool:
            logger.error("❌ The pool is not specified; the database cannot be written to")
            return

        for t, vol in daily_totals.items():
            await insert_daily_volume(self.pool, t, vol)

        logger.info(f"💾 {len(daily_totals)} days have been recorded in the database")


    async def fetch_user_balance(self, wallet_address: str, max_retries=3) -> float:
        endpoint = f"account?account={wallet_address}"

        for attempt in range(max_retries):
            result = await self._fetch(endpoint)

            if isinstance(result, dict) and "data" in result:
                return float(result["data"].get("available_to_spend", 0))

            if isinstance(result, int):
                wait_time = result
                logger.info(f"⏳ We'll wait {wait_time} seconds due to the limits before attempting {attempt + 2}...")
                await asyncio.sleep(wait_time)
                continue

            if result is None:
                await asyncio.sleep(1)

        logger.error(f"❌ Unable to retrieve the balance for {wallet_address[:6]} after {max_retries} attempts.")
        return 0.0

    async def fetch_user_positions(self, wallet_address: str, max_retries: int=3) -> dict:
        endpoint = f"positions?account={wallet_address}"

        for attempt in range(max_retries):
            result = await self._fetch(endpoint)

            if isinstance(result, int):
                logger.warning(f"⏳ Rate limit, waiting for {result} for ({attempt + 1} attempts)")
                await asyncio.sleep(result)
                continue

            if result is None:
                await asyncio.sleep(1)
                continue

            raw_positions = result.get('data', [])
            if not raw_positions:
                return {}

            positions = {}

            for pos in raw_positions:
                try:
                    symbol = pos['symbol']
                    amount = float(pos['amount'])
                    entry = float(pos['entry_price'])

                    positions[symbol] = {
                        "side": pos['side'],  # bid=long, ask=short
                        "amount": amount,
                        "entry": entry,
                        "value": amount * entry,  # USD value
                        "margin": float(pos.get('margin', 0))
                         #"margin": "0", // only shown for isolated margin
                    }
                except (KeyError, ValueError) as e:
                    logger.warning(f"⚠️ Unable to parse position {pos}: {e}")
                    continue
            logger.debug(f"📊 Positions {wallet_address[:6]}: {list(positions.keys())}")
            return positions
        logger.error(f"❌ Unable to retrieve positions for {wallet_address[:6]} after {max_retries} attempts")
        return {}

pacifica_client = PacificaClient()
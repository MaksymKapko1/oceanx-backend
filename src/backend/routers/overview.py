import asyncio

from fastapi import APIRouter, Request

from db.volume import get_historical_volume
from db.liquidations import get_24h_liquidations_volume, get_historical_liquidations
from services.pacifica_client import pacifica_client

router = APIRouter(
    prefix='/api/stats',
    tags=['Market Stats']
)

@router.get('/overview')
async def get_market_overview(request: Request):
    pool = request.app.state.db_pool

    daily_volume = pacifica_client.cache.get('daily_volume', 0.0)
    open_interest = pacifica_client.cache.get('open_interest', 0.0)

    top_volume = pacifica_client.cache.get('top_volume', [])
    top_oi = pacifica_client.cache.get('top_oi', [])

    liquidations_24h = await get_24h_liquidations_volume(pool)

    return {
        "success": True,
        "data": {
            'volume_24h': daily_volume,
            'open_interest': open_interest,
            'liquidations_24h': liquidations_24h,
            'active_markets': len(pacifica_client.cache.get('markets', [])),
            'market_symbols': pacifica_client.cache.get('markets', []),
            'top_volume': top_volume,
            'top_oi': top_oi,
        }
    }


@router.get('/historical')
async def get_historical_chart(request: Request):
    pool = request.app.state.db_pool

    volume_data, liquidations_data = await asyncio.gather(
        get_historical_volume(pool), get_historical_liquidations(pool)
    )

    return {
        "success": True,
        "data": {
            'volume': volume_data,
            'liquidations': liquidations_data,
        }
    }


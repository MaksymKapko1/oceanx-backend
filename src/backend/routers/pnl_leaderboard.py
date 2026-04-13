import logging
from fastapi import APIRouter, HTTPException, Query, Request

from services.pacifica_client import pacifica_client
from services.position_fetcher import fetch_acc_positions

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix='/api/leaderboard',
    tags=['Leaderboard']
)

@router.get('')
async def get_pnl_1d_leaderboard(
        request: Request,
        user_wallet: str = Query(default=None),
        search: str = Query(default=None),
        period: str = Query(default="1d"),
        sort_order: str = Query(default="desc"),
        limit: int = Query(default=50),
        offset: int = Query(default=0)
):
    all_active = pacifica_client.cache.get('pnl_1d_leaderboard', [])

    if not all_active:
        raise HTTPException(status_code=503, detail="Loading...")

    sort_field = f"pnl_{period}" if period != "all_time" else "pnl_all_time"
    valid_traders = [x for x in all_active if float(x.get(sort_field) or 0) != 0]

    is_reverse_sort = sort_order == "desc"
    sorted_list = sorted(valid_traders, key=lambda x: float(x.get(sort_field) or 0), reverse=is_reverse_sort)

    for i, trader in enumerate(sorted_list):
        trader["global_rank"] = i + 1

    if search:
        search_query = search.lower()
        sorted_list = [
            x for x in sorted_list
            if search_query in (x.get("address") or "").lower()
               or search_query in (x.get("username") or "").lower()
        ]

    paginated_data = sorted_list[offset: offset + limit]
    for master in paginated_data:
        master["is_followed"] = False

    if user_wallet and paginated_data:
        pool = request.app.state.db_pool

        master_addresses = [m.get("address") for m in paginated_data if m.get("address")]

        if master_addresses:
            query = """
                SELECT master_wallet, is_reverse
                FROM subscriptions
                WHERE user_id = (SELECT id FROM users WHERE wallet_address = $1)
                    AND master_wallet = ANY($2::text[])
                    AND is_active = TRUE;
            """
            try:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(query, user_wallet, master_addresses)
                    subs_map = {row["master_wallet"]: row["is_reverse"] for row in rows}

                    for master in paginated_data:
                        addr = master.get("address")
                        if addr in subs_map:
                            master["is_followed"] = True
                            master["is_reverse"] = subs_map[addr]
            except Exception as e:
                logger.error(f"Ошибка проверки подписок для лидерборда: {e}")

    return {
        "success": True,
        "total": len(sorted_list),
        "data": paginated_data
    }

@router.get("/{address}/positions")
async def get_trader_positions(address: str):
    try:
        positions = await fetch_acc_positions(address)
        return {"success": True, "data": positions}
    except Exception as e:
        logger.error(f"Ошибка получения позиций для {address}: {e}")
        return {"success": False, "data": []}

@router.get('/prices')
async def get_current_prices():
    markets_data = pacifica_client.cache.get('top_volume', [])
    mark_prices = {m['symbol']: m['mark_price'] for m in markets_data}
    return {"success": True, "data": mark_prices}
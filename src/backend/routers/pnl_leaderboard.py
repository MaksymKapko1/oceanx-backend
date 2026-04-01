import logging
from fastapi import APIRouter, HTTPException, Query

from services.pacifica_client import pacifica_client

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix='/api/leaderboard',
    tags=['Leaderboard']
)

@router.get('')
async def get_pnl_1d_leaderboard(
    period: str = Query(default="1d", description="1d | 7d | 30d | all_time"),
    limit: int = Query(default=50, ge=1, le=500, description="Кол-во трейдеров"),
    offset: int = Query(default=0, ge=0, description='Pagination')
):
    cached = pacifica_client.cache.get('pnl_1d_leaderboard', [])
    if not cached:
        logger.warning('⚠️ Кэш лидерборда пуст — данные ещё не загружены')
        raise HTTPException(
            status_code=503,
            detail="Данные ещё загружаются, попробуй через несколько секунд"
        )

    if period != "1d":
        sort_field = f"pnl_{period}" if period != "all_time" else "pnl_all_time"
        cached = sorted(
            cached,
            key=lambda x: float(x.get(sort_field) or 0),
            reverse=True
        )

    return {
        "success": True,
        "period": period,
        "total": len(cached),
        "returned": min(limit, len(cached)),
        "data": cached[offset: offset + limit]
    }
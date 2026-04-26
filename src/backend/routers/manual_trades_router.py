from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from services.manual_trade_service import ManualTradeService
from fastapi import Depends
from core.dependencies import get_active_wallet

router = APIRouter(prefix="/api/manual-trades", tags=["Manual Trades"])

class ClosePositionRequest(BaseModel):
    symbol: str

class OpenPositionRequest(BaseModel):
    symbol: str
    side: str
    size_usd: float

class HedgeOrderRequest(BaseModel):
    long_symbol: str
    short_symbol: str
    size_usd: float

@router.post("/open")
async def api_open_position(req: OpenPositionRequest, request: Request, wallet: str = Depends(get_active_wallet)):
    db_pool = request.app.state.db_pool
    service = ManualTradeService(db_pool)

    result = await service.open_position(wallet, req.symbol, req.side, req.size_usd)
    if not result['success']:
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
    return result

@router.post("/hedge")
async def api_execute_hedge(req: HedgeOrderRequest, request: Request, wallet: str = Depends(get_active_wallet)):
    db_pool = request.app.state.db_pool
    service = ManualTradeService(db_pool)

    result = await service.execute_hedge(wallet, req.long_symbol, req.short_symbol, req.size_usd)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
    return result

@router.post("/close")
async def api_close_position(req: ClosePositionRequest, request: Request, wallet: str = Depends(get_active_wallet)):
    db_pool = request.app.state.db_pool
    service = ManualTradeService(db_pool)

    result = await service.close_position(wallet, req.symbol)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
    return result

@router.post("/close-all")
async def api_close_all_positions(request: Request, wallet: str = Depends(get_active_wallet)):
    db_pool = request.app.state.db_pool
    service = ManualTradeService(db_pool)

    result = await service.close_all_positions(wallet)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
    return result
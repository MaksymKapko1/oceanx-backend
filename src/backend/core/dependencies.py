import os
import json
import time

import requests
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt

security = HTTPBearer()

PRIVY_APP_ID = os.getenv("PRIVY_APP_ID")
JWKS_URL = f"https://auth.privy.io/api/v1/apps/{PRIVY_APP_ID}/jwks.json"

_PRIVY_JWKS = None
JWKS_TTL = 86400
_JWKS_LAST_FETCH = 0


def get_jwks(force_refresh=False):
    global _PRIVY_JWKS, _JWKS_LAST_FETCH
    now = time.time()

    if _PRIVY_JWKS is None or (now - _JWKS_LAST_FETCH) > JWKS_TTL or force_refresh:
        try:
            response = requests.get(JWKS_URL, timeout=10)
            response.raise_for_status()
            _PRIVY_JWKS = response.json()
            _JWKS_LAST_FETCH = now
            print("🔄 JWKS Keys updated from Privy")
        except Exception as e:
            if _PRIVY_JWKS is None:
                raise HTTPException(status_code=500, detail="Auth keys unavailable")
    return _PRIVY_JWKS


def verify_privy_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> dict:
    token = credentials.credentials
    try:
        return jwt.decode(token, get_jwks(), algorithms=["ES256"],
                          issuer="privy.io", audience=PRIVY_APP_ID)
    except jwt.JWTError:
        try:
            return jwt.decode(token, get_jwks(force_refresh=True), algorithms=["ES256"],
                              issuer="privy.io", audience=PRIVY_APP_ID)
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid Token")


def get_active_wallet(token_data: dict = Depends(verify_privy_token)) -> str:
    accounts_raw = token_data.get("linked_accounts", "[]")
    linked_accounts = json.loads(accounts_raw)

    sol_wallet = next(
        (acc.get("address") for acc in linked_accounts
         if acc.get("type") == "wallet" and acc.get("chain_type") == "solana"),
        None
    )

    if not sol_wallet:
        raise HTTPException(status_code=401, detail="No Solana wallet linked")

    return sol_wallet
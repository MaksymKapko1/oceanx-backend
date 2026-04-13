
import time
import json
import requests
from solders.keypair import Keypair
import sys
sys.path.append('/Users/maksym/Desktop/fast_pacifica/src/backend')
from common.utils import sign_message  # ← просто импортируем готовое

REST_URL = "https://api.pacifica.fi/api/v1"
PRIVATE_KEY = ""


def main():
    keypair = Keypair.from_base58_string(PRIVATE_KEY)
    public_key = str(keypair.pubkey())

    timestamp = int(time.time() * 1000)
    header = {
        "timestamp": timestamp,
        "expiry_window": 5000,
        "type": "create_api_key",
    }
    payload = {}

    message, signature = sign_message(header, payload, keypair)
    print(f"Message: {message}")
    print(f"Signature: {signature}")

    request_body = {
        "account": public_key,
        "agent_wallet": None,
        "signature": signature,
        "timestamp": header["timestamp"],
        "expiry_window": header["expiry_window"],
    }

    response = requests.post(
        f"{REST_URL}/account/api_keys/create",
        json=request_body,
        headers={"Content-Type": "application/json"}
    )

    print(f"\nHTTP {response.status_code}")
    print(f"Raw: {response.text}")


if __name__ == "__main__":
    main()
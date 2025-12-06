from __future__ import annotations

import asyncio
from datetime import datetime

from app import load_settings
from app.io import TradingViewClient

SAMPLE_TRANSACTION = {
    "transaction_id": "11091",
    "user_id": "253575443",
    "product_id": "2291",
    "subscription_id": "5327",
    "amount": "56.00",
    "total": "56.00",
    "txn_status": "Complete",
    "txn_type": "Renewal",
    "created_at": "2025-10-27 15:45:20",
    "expires_at": "2026-11-27 23:59:59",
    "trans_num": "sub_RYYFfQxrGtAiUB",
    "gateway": "syg5wj-5x0",
    "user_email": "muhamad@gmail.com",
    "user_login": "tanmay_jadav",
    "display_name": "Tanmay Jadav",
    "user_registered": "2025-10-27 15:45:20",
    "subscription_status": "pending",
    "subscription_created_at": "2025-10-27 15:45:20",
    "subscription_price": "56.00",
    "user_meta": {
        "first_name": "Muhamad Ferdian",
        "last_name": "Syah",
        "phone": "",
        "country": "",
    },
}


async def run() -> None:
    settings = load_settings()
    product_cfg = settings.product_for(SAMPLE_TRANSACTION["product_id"])
    if product_cfg is None:
        raise SystemExit(
            f"Product {SAMPLE_TRANSACTION['product_id']} not found in config.json."
        )

    tv_client = TradingViewClient(settings)

    expiry = datetime.strptime(
        SAMPLE_TRANSACTION["expires_at"], "%Y-%m-%d %H:%M:%S"
    ).date()

    payload = {
        "scriptId": product_cfg.script_id,
        "username": SAMPLE_TRANSACTION["user_login"],
        "email": SAMPLE_TRANSACTION["user_email"],
        "expiry": expiry.isoformat(),
        "subscription_type": product_cfg.subscription_type or "",
        "wp_username": SAMPLE_TRANSACTION["user_login"],
        "remarks": SAMPLE_TRANSACTION.get("txn_status", "paid"),
    }

    print("Sending payload:", payload)
    response = await tv_client.grant_access(payload)
    print("TradingView response:", response)


if __name__ == "__main__":
    asyncio.run(run())

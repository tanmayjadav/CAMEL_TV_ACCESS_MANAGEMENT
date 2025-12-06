from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.config import (
    LoggingConfig,
    PathConfig,
    ProductConfig,
    SchedulerConfig,
    Settings,
    TradingViewConfig,
    WordPressConfig,
    EmailConfig,
)


@pytest.fixture()
def sample_settings(tmp_path) -> Settings:
    master_dir = tmp_path / "masterData"
    logs_dir = tmp_path / "logs"
    master_dir.mkdir()
    logs_dir.mkdir()
    return Settings(
        wordpress=WordPressConfig(
            base_url="https://example.com/wp-json",
            transactions_endpoint="/all-user-transactions",
            transactions_limit=10,
            status_filter=["complete"],
        ),
        tradingview=TradingViewConfig(
            base_url="https://example.com/api",
            grant_endpoint="/tradingview/access/grant",
            list_users_endpoint="/tradingview/access/scriptUsers/{scriptId}",
            api_key_header="x-api-key",
            api_key="test-key",
            timeout_seconds=5,
            max_retries=0,
        ),
        products={
            "2291": ProductConfig(
                script_id="SCRIPT_2291",
                duration_days=30,
                subscription_type="Monthly",
                stacking_allowed=True,
            )
        },
        scheduler=SchedulerConfig(interval_minutes=15),
        logging=LoggingConfig(level="INFO"),
        paths=PathConfig(
            masterdata_dir=str(master_dir),
            logs_dir=str(logs_dir),
        ),
        email=EmailConfig(
            smtp_server="smtp.example.com",
            smtp_port=465,
            smtp_user="noreply@example.com",
            smtp_password="secret",
            from_email="noreply@example.com",
        ),
    )


@pytest.fixture()
def sample_raw_transaction() -> dict:
    return {
        "transaction_id": "tx-1",
        "user_id": "123",
        "user_email": "user@example.com",
        "user_login": "user1",
        "display_name": "User One",
        "amount": "56.00",
        "total": "56.00",
        "txn_status": "complete",
        "txn_type": "payment",
        "subscription_id": "sub-1",
        "product_id": "2291",
        "gateway": "manual",
        "trans_num": "mp-txn-1",
        "created_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at": None,
    }


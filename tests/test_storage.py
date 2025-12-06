from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.storage import (
    AccessRecord,
    GrantHistoryEntry,
    MasterData,
    bootstrap_from_tradingview,
    load_master,
    save_master,
)


def test_save_and_load_master(sample_settings):
    master = MasterData(script_id="PUB_2291")
    processed_at = datetime.now(tz=timezone.utc)
    record = AccessRecord(
        wp_user_id="123",
        username="user1",
        wp_username="user1",
        email="user@example.com",
        product_id="2291",
        script_id="SCRIPT_2291",
        expiry=processed_at + timedelta(days=30),
        last_transaction_id="tx-1",
        last_transaction_at=processed_at,
        status="active",
        history=[
            GrantHistoryEntry(
                transaction_id="tx-1",
                action="grant_new",
                expires_at=processed_at + timedelta(days=30),
                processed_at=processed_at,
            )
        ],
    )
    master.record_user("user1", record)
    master.register_processed("tx-1")
    save_master(sample_settings, master)

    loaded = load_master(sample_settings, "PUB_2291")
    assert "user1" in loaded.users
    assert "tx-1" in loaded.processed_transactions


def test_bootstrap_from_tradingview(sample_settings):
    users = [
        {
            "username": "user2",
            "email": "user2@example.com",
            "expiry": (datetime.now(tz=timezone.utc) + timedelta(days=10)).isoformat(),
            "last_transaction_id": "tx-2",
            "last_transaction_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    ]
    master = bootstrap_from_tradingview(sample_settings, "SCRIPT_2291", users)
    assert "user2" in master.users
    assert master.users["user2"].script_id == "SCRIPT_2291"


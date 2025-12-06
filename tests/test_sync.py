from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app import sync
from app.storage import AccessRecord, GrantHistoryEntry, MasterData, load_master, save_master


class DummyWordPressClient:
    def __init__(self, settings):
        self.settings = settings
        self.since = None

    async def fetch_transactions(self, since=None):
        self.since = since
        return [
            {
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
                "created_at": "2025-11-18 11:14:59",
                "expires_at": None,
            }
        ]


class DummyTradingViewClient:
    def __init__(self, settings):
        self.settings = settings
        self.grants = []
        self.updates = []
        self.validations = []

    async def list_script_users(self, script_id: str):
        return []

    async def grant_access(self, payload):
        self.grants.append(payload)
        return {"status": "ok"}

    async def update_access(self, payload):
        self.updates.append(payload)
        return {"status": "ok"}

    async def validate_username(self, username: str):
        self.validations.append(username)
        return {"validUser": True, "verifiedUserName": username, "allUserSuggestions": []}


@pytest.mark.asyncio
async def test_run_sync_flow(sample_settings, monkeypatch):
    monkeypatch.setattr(sync, "WordPressClient", DummyWordPressClient)
    dummy_tv = DummyTradingViewClient(sample_settings)
    monkeypatch.setattr(sync, "TradingViewClient", lambda settings: dummy_tv)

    summary = await sync.run_sync(sample_settings)
    assert summary["processed"] == 1
    assert len(dummy_tv.grants) == 1
    grant_payload = dummy_tv.grants[0]
    assert grant_payload["username"] == "user1"
    assert grant_payload["wp_username"] == "user1"
    assert grant_payload["remarks"] == "paid"
    assert grant_payload["scriptId"] == "SCRIPT_2291"
    assert dummy_tv.validations == ["user1"]
    assert summary["dry_run_calls"] == 0


@pytest.mark.asyncio
async def test_run_sync_updates_existing(sample_settings, monkeypatch):
    transaction_time = datetime.fromisoformat("2025-11-18T11:14:59+00:00")
    initial_processed_at = transaction_time - timedelta(hours=1)
    existing_record = AccessRecord(
        wp_user_id="123",
        username="user1",
        email="user@example.com",
        product_id="2291",
        script_id="SCRIPT_2291",
        expiry=transaction_time + timedelta(days=10),
        last_transaction_id="prev-tx",
        last_transaction_at=transaction_time - timedelta(days=1),
        status="active",
        history=[
            GrantHistoryEntry(
                transaction_id="prev-tx",
                action="grant_new",
                expires_at=transaction_time + timedelta(days=10),
                processed_at=transaction_time - timedelta(days=1),
            )
        ],
    )
    master = MasterData(
        script_id="SCRIPT_2291",
        users={"user1": existing_record},
        processed_transactions=["prev-tx"],
        last_processed_at=initial_processed_at,
    )
    save_master(sample_settings, master)

    monkeypatch.setattr(sync, "WordPressClient", DummyWordPressClient)
    dummy_tv = DummyTradingViewClient(sample_settings)
    monkeypatch.setattr(sync, "TradingViewClient", lambda settings: dummy_tv)

    summary = await sync.run_sync(sample_settings)
    assert summary["stacked"] == 1
    assert len(dummy_tv.updates) == 1
    update_payload = dummy_tv.updates[0]
    assert update_payload["username"] == "user1"
    assert update_payload["wp_username"] == "user1"
    assert update_payload["remarks"] == "paid"
    assert update_payload["scriptId"] == "SCRIPT_2291"
    # Grant should not be called for existing stack scenario
    assert len(dummy_tv.grants) == 0
    assert summary["since"] == initial_processed_at.isoformat()

    updated_master = load_master(sample_settings, "SCRIPT_2291")
    assert updated_master.last_processed_at == transaction_time
    assert dummy_tv.validations == ["user1", "user1"]
    assert summary["dry_run_calls"] == 0


@pytest.mark.asyncio
async def test_run_sync_invalid_username_triggers_email(sample_settings, monkeypatch):
    class InvalidWordPressClient(DummyWordPressClient):
        async def fetch_transactions(self, since=None):
            base = await super().fetch_transactions(since)
            base[0]["txn_status"] = "complete"
            return base

    class InvalidTradingViewClient(DummyTradingViewClient):
        async def validate_username(self, username: str):
            self.validations.append(username)
            return {
                "validUser": False,
                "verifiedUserName": "",
                "allUserSuggestions": [{"username": f"{username}_tv"}],
            }

    email_calls = []

    async def fake_send_email(**kwargs):
        email_calls.append(kwargs)

    monkeypatch.setattr(sync, "WordPressClient", InvalidWordPressClient)
    dummy_tv = InvalidTradingViewClient(sample_settings)
    monkeypatch.setattr(sync, "TradingViewClient", lambda settings: dummy_tv)
    monkeypatch.setattr(sync, "send_email", fake_send_email)

    summary = await sync.run_sync(sample_settings)
    assert summary["manual_review"] == 1
    assert summary["processed"] == 0
    assert summary["stacked"] == 0
    assert dummy_tv.grants == []
    assert dummy_tv.updates == []
    assert dummy_tv.validations == ["user1"]
    assert len(email_calls) == 1
    email_kwargs = email_calls[0]
    assert email_kwargs["to_email"] == "user@example.com"

    master = load_master(sample_settings, "SCRIPT_2291")
    assert any(entry.reason == "invalid_username" for entry in master.manual_review)
    assert master.processed_transactions == ["tx-1"]


@pytest.mark.asyncio
async def test_run_sync_dry_run(sample_settings, monkeypatch):
    monkeypatch.setattr(sync, "WordPressClient", DummyWordPressClient)
    dummy_tv = DummyTradingViewClient(sample_settings)
    monkeypatch.setattr(sync, "TradingViewClient", lambda settings: dummy_tv)

    summary = await sync.run_sync(sample_settings, dry_run=True)
    assert summary["processed"] == 0
    assert summary["stacked"] == 0
    assert summary["dry_run_calls"] == 1
    assert summary["skipped"] == 1
    assert dummy_tv.grants == []
    assert dummy_tv.updates == []

    master = load_master(sample_settings, "SCRIPT_2291")
    # No state mutation should occur during dry run
    assert master.users == {}
    assert master.processed_transactions == []


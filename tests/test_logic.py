from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.logic import derive_action, normalize_transactions
from app.storage import AccessRecord, GrantHistoryEntry


def test_normalize_transactions(sample_settings, sample_raw_transaction):
    results = normalize_transactions([sample_raw_transaction], sample_settings)
    assert len(results) == 1
    txn = results[0]
    assert txn.script_id == "SCRIPT_2291"
    assert txn.username == sample_raw_transaction["user_login"]
    assert txn.wp_username == sample_raw_transaction["user_login"]
    assert txn.remarks == "paid"


def test_derive_action_new_user(sample_settings, sample_raw_transaction):
    txn = normalize_transactions([sample_raw_transaction], sample_settings)[0]
    action = derive_action(txn, None)
    assert action.type == "grant_new"
    assert action.expires_at is not None


def test_derive_action_stack_existing(sample_settings, sample_raw_transaction):
    txn = normalize_transactions([sample_raw_transaction], sample_settings)[0]
    existing = AccessRecord(
        wp_user_id="123",
        username=txn.username,
        wp_username=txn.wp_username,
        email=txn.email,
        product_id=txn.product_id,
        script_id=txn.script_id,
        expiry=datetime.now(tz=timezone.utc) + timedelta(days=10),
        last_transaction_id="prev",
        last_transaction_at=datetime.now(tz=timezone.utc) - timedelta(days=1),
        status="active",
        history=[
            GrantHistoryEntry(
                transaction_id="prev",
                action="grant_new",
                expires_at=datetime.now(tz=timezone.utc) + timedelta(days=10),
                processed_at=datetime.now(tz=timezone.utc),
            )
        ],
    )
    action = derive_action(txn, existing)
    assert action.type == "stack_existing"
    assert action.expires_at > existing.expiry


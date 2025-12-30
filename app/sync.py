from __future__ import annotations

import logging
import pathlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .config import Settings, get_settings
from .io import ApiError, TradingViewClient, WordPressClient
from .logic import NormalizedTransaction, derive_action, normalize_transactions
from .storage import (
    AccessRecord,
    GrantHistoryEntry,
    ManualReviewEntry,
    MasterData,
    RetryEntry,
    bootstrap_from_tradingview,
    load_master,
    save_master,
)
from .email import send_email

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


INVALID_TEMPLATE_PATH = pathlib.Path(__file__).parent / "templates" / "invalid_username.html"
_INVALID_TEMPLATE_CACHE: Optional[str] = None


def _render_invalid_username_email(
    username: str, suggestions: List[Dict[str, Any]]
) -> str:
    global _INVALID_TEMPLATE_CACHE
    if _INVALID_TEMPLATE_CACHE is None:
        _INVALID_TEMPLATE_CACHE = INVALID_TEMPLATE_PATH.read_text(encoding="utf-8")

    if suggestions:
        items = "".join(
            f'<li style="color: #160c66; font-size: 15px;"><strong>{s.get("username")}</strong></li>'
            for s in suggestions
            if s.get("username")
        )
        if items:
            suggestion_block = (
                '<h3 style="color: #ff8f0f; font-size: 20px;">Did you mean?</h3>'
                f'<ul style="color: #160c66; font-size: 15px; padding-left: 20px;">{items}</ul>'
            )
        else:
            suggestion_block = (
                '<p style="color: #160c66; font-size: 15px;">We could not find close matches for '
                "your username. Please double-check it inside TradingView.</p>"
            )
    else:
        suggestion_block = (
            '<p style="color: #160c66; font-size: 15px;">TradingView did not return any suggestions '
            "for the username you entered.</p>"
        )

    return _INVALID_TEMPLATE_CACHE.format(username=username, suggestions=suggestion_block)


async def _send_invalid_username_email(
    settings: Settings,
    txn: NormalizedTransaction,
    suggestions: List[Dict[str, Any]],
) -> None:
    if not settings.email:
        logger.warning(
            "email.disabled",
            extra={
                "extra_data": {
                    "reason": "missing_email_config",
                    "username": txn.username,
                    "recipient": txn.email,
                }
            },
        )
        return

    html = _render_invalid_username_email(txn.username, suggestions)
    try:
        await send_email(
            to_email=txn.email,
            subject="Action needed: update your TradingView username",
            html_body=html,
            from_email=settings.email.from_email,
            smtp_server=settings.email.smtp_server,
            smtp_port=settings.email.smtp_port,
            smtp_user=settings.email.smtp_user,
            smtp_password=settings.email.smtp_password,
            bcc=settings.email.bcc or [],
        )
    except Exception as exc:  # pragma: no cover - logged in send_email
        logger.error(
            "email.send_failed",
            extra={
                "extra_data": {
                    "username": txn.username,
                    "recipient": txn.email,
                    "error": str(exc),
                }
            },
        )


def _grant_payload(
    txn: NormalizedTransaction, expiry: datetime, username: str
) -> Dict:
    payload = {
        "scriptId": txn.script_id,
        "username": username,
        "email": txn.email,
        "expiry": expiry.date().isoformat(),
        "subscription_type": txn.subscription_type or "",
        "wp_username": txn.wp_username,
        "remarks": txn.remarks or "paid",
    }
    return payload


async def run_sync(settings: Optional[Settings] = None) -> Dict:
    settings = settings or get_settings()
    wp_client = WordPressClient(settings)
    tv_client = TradingViewClient(settings)
    dry_run = settings.scheduler.dry_run

    async def load_or_bootstrap(script_id: str) -> MasterData:
        master = load_master(settings, script_id)
        # Bootstrap from TradingView is disabled per current requirements.
        # Previously we fetched existing script users here so the local masterData file would
        # start with TradingView’s current state. If we need that behavior again, uncomment:
        #
        # if not master.users and master.last_synced_at is None:
        #     try:
        #         users = await tv_client.list_script_users(script_id)
        #         master = bootstrap_from_tradingview(settings, script_id, users)
        #         logger.info(
        #             "masterdata.bootstrap",
        #             extra={"extra_data": {"scriptId": script_id, "count": len(users)}},
        #         )
        #     except ApiError as exc:
        #         logger.warning(
        #             "masterdata.bootstrap_failed",
        #             extra={
        #                 "extra_data": {
        #                     "scriptId": script_id,
        #                     "error": str(exc),
        #                 }
        #             },
        #         )
        return master

    script_ids = {product.script_id for product in settings.products.values()}
    master_cache: Dict[str, MasterData] = {}
    for script_id in script_ids:
        master_cache[script_id] = await load_or_bootstrap(script_id)

    since_candidates = [
        master.last_processed_at for master in master_cache.values() if master.last_processed_at
    ]
    since_timestamp = min(since_candidates) if since_candidates else None

    async def execute_tv_action(action_type: str, payload: Dict, summary: Dict) -> None:
        if dry_run:
            summary["dry_run_calls"] += 1
            logger.info(
                "dry_run.tradingview_call",
                extra={"extra_data": {"action": action_type, "payload": payload}},
            )
            return
        # Use grant_access for all actions since the API only has a grant endpoint
        # The grant endpoint can handle both new grants and updates/extensions
        await tv_client.grant_access(payload)

    raw_transactions = await wp_client.fetch_transactions(since=since_timestamp)
    normalized = normalize_transactions(raw_transactions, settings)

    summary = {
        "transactions_fetched": len(raw_transactions),
        "transactions_considered": len(normalized),
        "processed": 0,
        "stacked": 0,
        "skipped": 0,
        "manual_review": 0,
        "failed": 0,
        "dry_run_skipped": 0,
        "since": since_timestamp.isoformat() if since_timestamp else None,
        "dry_run_calls": 0,
        "validation_failed": 0,
    }

    latest_seen: Dict[str, datetime] = {}

    separator_line = "*" * 98

    async def get_master(script_id: str) -> MasterData:
        if script_id in master_cache:
            return master_cache[script_id]
        master = await load_or_bootstrap(script_id)
        master_cache[script_id] = master
        return master

    total_transactions = len(normalized)

    for index, txn in enumerate(normalized, start=1):
        logger.info(separator_line)
        logger.info(
            f"Processing user {txn.wp_user_id} ({index}/{total_transactions})"
        )
        logger.info(
            "TV username=%s email=%s product_id=%s script_id=%s ",
            txn.username,
            txn.email,
            txn.product_id,
            txn.script_id,
        )

        master = await get_master(txn.script_id)

        current_seen = latest_seen.get(txn.script_id)
        if current_seen is None or txn.created_at > current_seen:
            latest_seen[txn.script_id] = txn.created_at

        if txn.transaction_id in master.processed_transactions:
            summary["skipped"] += 1
            logger.info(
                "Skipping transaction %s (already processed)",
                txn.transaction_id,
            )
            continue

        existing = master.users.get(txn.username)
        action = derive_action(txn, existing)

        if action.type == "skip":
            summary["skipped"] += 1
            master.register_processed(txn.transaction_id)
            logger.info(
                "Transaction %s skipped (%s)",
                txn.transaction_id,
                action.reason or "reason not specified",
            )
            continue

        if action.type == "manual_review":
            summary["manual_review"] += 1
            master.record_manual_review(
                ManualReviewEntry(
                    transaction_id=txn.transaction_id,
                    reason=action.reason or "manual_review_required",
                )
            )
            master.register_processed(txn.transaction_id)
            logger.warning(
                "Transaction %s moved to manual review (%s)",
                txn.transaction_id,
                action.reason or "manual review",
            )
            continue

        if not action.expires_at:
            summary["failed"] += 1
            logger.error(
                "Transaction %s has no expiry; skipping", txn.transaction_id
            )
            continue

        try:
            validation_result = await tv_client.validate_username(txn.username)
        except ApiError as exc:
            summary["validation_failed"] += 1
            logger.error(
                "Validation error for %s (%s)", txn.username, txn.transaction_id
            )
            continue

        if not validation_result.get("validUser"):
            if not any(
                entry.transaction_id == txn.transaction_id for entry in master.manual_review
            ):
                await _send_invalid_username_email(
                    settings,
                    txn,
                    validation_result.get("allUserSuggestions", []),
                )
                master.record_manual_review(
                    ManualReviewEntry(
                        transaction_id=txn.transaction_id,
                        reason="invalid_username",
                    )
                )
            summary["manual_review"] += 1
            master.register_processed(txn.transaction_id)
            logger.warning(
                "TradingView returned invalid user for %s (%s)",
                txn.username,
                txn.transaction_id,
            )
            continue

        effective_username = (
            validation_result.get("verifiedUserName") or txn.username
        )

        if effective_username != txn.username:
            existing = master.users.get(effective_username) or existing

        payload = _grant_payload(txn, action.expires_at, effective_username)

        if dry_run:
            logger.info(
                "Dry run: would call TradingView %s for %s",
                action.type,
                effective_username,
            )
            summary["dry_run_skipped"] += 1
            continue
        else:
            try:
                await execute_tv_action(action.type, payload, summary)
            except ApiError as exc:
                summary["failed"] += 1
                master.record_retry(
                    RetryEntry(
                        transaction_id=txn.transaction_id,
                        payload=payload,
                        error_message=str(exc),
                        attempts=0,
                    )
                )
                logger.error(
                    "TradingView call failed for %s (%s)",
                    effective_username,
                    txn.transaction_id,
                )
                continue

        if dry_run:
            logger.info(
                "Dry run: skipping state update for %s", effective_username
            )
            summary["skipped"] += 1
            continue

        processed_at = _utcnow()

        history = list(existing.history) if existing else []
        history.append(
            GrantHistoryEntry(
                transaction_id=txn.transaction_id,
                action=action.type,
                expires_at=action.expires_at,
                processed_at=processed_at,
            )
        )

        record = AccessRecord(
            wp_user_id=txn.wp_user_id,
            username=effective_username,
            wp_username=txn.wp_username,
            email=txn.email,
            product_id=txn.product_id,
            script_id=txn.script_id,
            expiry=action.expires_at,
            last_transaction_id=txn.transaction_id,
            last_transaction_at=txn.created_at,
            status="active",
            history=history,
        )
        if effective_username != txn.username:
            master.users.pop(txn.username, None)
        master.record_user(effective_username, record)
        master.register_processed(txn.transaction_id)
        master.last_synced_at = processed_at

        if action.type == "grant_new":
            summary["processed"] += 1
        else:
            summary["stacked"] += 1

        logger.info(
            "Processed transaction %s action=%s expiry=%s",
            txn.transaction_id,
            action.type,
            action.expires_at.isoformat(),
        )

    for script_id, master in master_cache.items():
        candidate = latest_seen.get(script_id)
        if candidate and (
            master.last_processed_at is None or candidate > master.last_processed_at
        ):
            master.last_processed_at = candidate

    for master in master_cache.values():
        save_master(settings, master)

    logger.info("sync.completed", extra={"extra_data": summary})
    return summary


from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field, ValidationError

from .config import ProductConfig, Settings
from .storage import AccessRecord

logger = logging.getLogger(__name__)


def _parse_datetime(value: str) -> datetime:
    if not value:
        raise ValueError("Empty datetime value")

    candidate = value.strip()
    attempts = [
        lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f"),
    ]

    for attempt in attempts:
        try:
            dt = attempt(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    raise ValueError(f"Invalid datetime format: {value}")


class WordPressUser(BaseModel):
    id: Optional[str] = None
    email: Optional[str] = None
    username: Optional[str] = None
    display_name: Optional[str] = None


class WordPressTransaction(BaseModel):
    transaction_id: str
    user: Optional[WordPressUser] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    user_login: Optional[str] = None
    display_name: Optional[str] = None
    user_meta: Optional[Dict[str, Any]] = None
    amount: Optional[float] = None
    total: Optional[float] = None
    status: Optional[str] = None
    txn_status: Optional[str] = None
    type: Optional[str] = None
    txn_type: Optional[str] = None
    subscription_id: Optional[str] = None
    parent_transaction_id: Optional[str] = None
    product_id: str
    gateway: Optional[str] = None
    transaction_number: Optional[str] = Field(default=None, alias="trans_num")
    created_at: str
    expires_at: Optional[str] = None
    subscription_status: Optional[str] = None

    @property
    def created_at_dt(self) -> datetime:
        return _parse_datetime(self.created_at)

    @property
    def expires_at_dt(self) -> Optional[datetime]:
        if not self.expires_at:
            return None
        try:
            return _parse_datetime(self.expires_at)
        except ValueError:
            return None


class NormalizedTransaction(BaseModel):
    transaction_id: str
    product_id: str
    script_id: str
    username: str
    email: str
    wp_username: str
    wp_user_id: str
    display_name: Optional[str] = None
    created_at: datetime
    computed_expiry: datetime
    duration_days: int
    subscription_type: Optional[str] = None
    stacking_allowed: bool = True
    remarks: Optional[str] = None
    raw: Dict


class Action(BaseModel):
    type: str
    expires_at: Optional[datetime] = None
    reason: Optional[str] = None


def normalize_transactions(
    raw_transactions: Iterable[Dict],
    settings: Settings,
) -> List[NormalizedTransaction]:
    normalized: List[NormalizedTransaction] = []
    allowed_statuses = {status.lower() for status in settings.wordpress.status_filter}

    for raw in raw_transactions:
        try:
            transaction = WordPressTransaction.model_validate(raw)
        except ValidationError as exc:
            logger.warning(
                "transaction.validation_failed",
                extra={
                    "extra_data": {
                        "errors": exc.errors(),
                        "transaction": raw.get("transaction_id"),
                    }
                },
            )
            continue

        status_value = (
            transaction.status
            or transaction.txn_status
            or raw.get("status")
            or raw.get("txn_status")
            or ""
        ).strip()
        if allowed_statuses and status_value.lower() not in allowed_statuses:
            continue

        product = settings.product_for(transaction.product_id)
        if not product:
            logger.warning(
                "transaction.unknown_product",
                extra={
                    "extra_data": {
                        "transactionId": transaction.transaction_id,
                        "productId": transaction.product_id,
                    }
                },
            )
            continue

        try:
            created_at = transaction.created_at_dt
        except ValueError:
            logger.warning(
                "transaction.invalid_created_at",
                extra={
                    "extra_data": {
                        "transactionId": transaction.transaction_id,
                        "created_at": transaction.created_at,
                    }
                },
            )
            continue

        computed_expiry = compute_expiry(
            created_at,
            product.duration_days,
            transaction.expires_at_dt,
        )

        username = (
            transaction.user.username if transaction.user and transaction.user.username else None
        ) or transaction.user_login or raw.get("user_login")
        if not username:
            logger.warning(
                "transaction.missing_username",
                extra={
                    "extra_data": {
                        "transactionId": transaction.transaction_id,
                        "productId": transaction.product_id,
                    }
                },
            )
            continue

        email = (
            transaction.user.email if transaction.user and transaction.user.email else None
        ) or transaction.user_email or raw.get("user_email")
        if not email:
            logger.warning(
                "transaction.missing_email",
                extra={
                    "extra_data": {
                        "transactionId": transaction.transaction_id,
                        "productId": transaction.product_id,
                    }
                },
            )
            continue

        wp_user_id = (
            transaction.user.id if transaction.user and transaction.user.id else None
        ) or transaction.user_id or raw.get("user_id")
        if not wp_user_id:
            logger.warning(
                "transaction.missing_user_id",
                extra={
                    "extra_data": {
                        "transactionId": transaction.transaction_id,
                        "productId": transaction.product_id,
                    }
                },
            )
            continue

        display_name = (
            transaction.user.display_name if transaction.user and transaction.user.display_name else None
        ) or transaction.display_name or raw.get("display_name")
        if not display_name and transaction.user_meta:
            first = transaction.user_meta.get("first_name")
            last = transaction.user_meta.get("last_name")
            parts = [part for part in [first, last] if part]
            if parts:
                display_name = " ".join(parts)

        remarks = raw.get("remarks") or raw.get("note")
        if not remarks:
            remarks = "paid" if status_value.lower() == "complete" else status_value or "paid"

        normalized.append(
            NormalizedTransaction(
                transaction_id=transaction.transaction_id,
                product_id=transaction.product_id,
                script_id=product.script_id,
                username=username,
                email=email,
                wp_username=username,
                wp_user_id=str(wp_user_id),
                display_name=display_name,
                created_at=created_at,
                computed_expiry=computed_expiry,
                duration_days=product.duration_days,
                subscription_type=product.subscription_type,
                stacking_allowed=product.stacking_allowed,
                remarks=remarks,
                raw=raw,
            )
        )
    return normalized


def compute_expiry(
    created_at: datetime,
    duration_days: int,
    source_expiry: Optional[datetime] = None,
) -> datetime:
    if source_expiry:
        return source_expiry.astimezone(timezone.utc)
    return created_at.astimezone(timezone.utc) + timedelta(days=duration_days)


def derive_action(
    transaction: NormalizedTransaction,
    existing_record: Optional[AccessRecord],
) -> Action:
    if existing_record is None:
        return Action(type="grant_new", expires_at=transaction.computed_expiry)

    if transaction.transaction_id == existing_record.last_transaction_id:
        return Action(
            type="skip",
            reason="duplicate_transaction",
            expires_at=existing_record.expiry,
        )

    existing_expiry = existing_record.expiry

    if transaction.stacking_allowed:
        if existing_expiry and existing_expiry > transaction.created_at:
            new_expiry = existing_expiry + timedelta(days=transaction.duration_days)
            return Action(type="stack_existing", expires_at=new_expiry)
        return Action(type="stack_existing", expires_at=transaction.computed_expiry)

    # Stacking not allowed: defer to manual review so we don't truncate access silently.
    return Action(
        type="manual_review",
        reason="stacking_disabled_existing_access",
        expires_at=existing_expiry,
    )

'''derive_action(transaction, existing_record): core decision engine.
If no existing record: return grant_new.
If the same transaction already processed: return skip to keep idempotence.
Otherwise, evaluate stacking:
If stacking allowed and existing expiry > new transaction’s created_at → extend existing expiry by duration (stack).
If stacking allowed but already expired → treat as fresh (use computed expiry).
If stacking disabled → flag for manual review (don’t auto-shorten).
Returns an Action with type, new expiry, and reason if applicable.'''
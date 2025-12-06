from __future__ import annotations

import json
import logging
import pathlib
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .config import Settings

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class GrantHistoryEntry(BaseModel):
    transaction_id: str
    action: str
    expires_at: datetime
    processed_at: datetime
    note: Optional[str] = None

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}


class RetryEntry(BaseModel):
    transaction_id: str
    payload: Dict
    error_message: str
    attempts: int = 0
    next_attempt_at: datetime = Field(default_factory=_utcnow)

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}


class ManualReviewEntry(BaseModel):
    transaction_id: str
    reason: str
    recorded_at: datetime = Field(default_factory=_utcnow)

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}


class AccessRecord(BaseModel):
    wp_user_id: str
    username: str
    wp_username: Optional[str] = None
    email: str
    product_id: str
    script_id: str
    expiry: datetime
    last_transaction_id: str
    last_transaction_at: datetime
    status: str = "active"
    history: List[GrantHistoryEntry] = Field(default_factory=list)

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}


class MasterData(BaseModel):
    script_id: str
    last_synced_at: Optional[datetime] = None
    last_processed_at: Optional[datetime] = None
    processed_transactions: List[str] = Field(default_factory=list)
    users: Dict[str, AccessRecord] = Field(default_factory=dict)
    retry_queue: List[RetryEntry] = Field(default_factory=list)
    manual_review: List[ManualReviewEntry] = Field(default_factory=list)

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}

    def register_processed(self, transaction_id: str) -> None:
        if transaction_id not in self.processed_transactions:
            self.processed_transactions.append(transaction_id)
            if len(self.processed_transactions) > 500:
                self.processed_transactions = self.processed_transactions[-500:]

    def record_user(self, username: str, record: AccessRecord) -> None:
        if record.wp_username is None:
            record.wp_username = record.username
        self.users[username] = record

    def record_retry(self, entry: RetryEntry) -> None:
        self.retry_queue.append(entry)

    def record_manual_review(self, entry: ManualReviewEntry) -> None:
        self.manual_review.append(entry)


def _master_path(settings: Settings, script_id: str) -> pathlib.Path:
    return settings.masterdata_path / f"{script_id}.json"


def load_master(settings: Settings, script_id: str) -> MasterData:
    path = _master_path(settings, script_id)
    if not path.exists():
        logger.info(
            "masterdata.init",
            extra={"extra_data": {"scriptId": script_id, "path": str(path)}},
        )
        return MasterData(script_id=script_id)

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    master = MasterData.model_validate(data)
    return master


def save_master(settings: Settings, master: MasterData) -> None:
    path = _master_path(settings, master.script_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = master.model_dump(mode="json")
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    logger.debug(
        "masterdata.saved",
        extra={"extra_data": {"scriptId": master.script_id, "path": str(path)}},
    )


def bootstrap_from_tradingview(
    settings: Settings, script_id: str, tv_users: List[Dict]
) -> MasterData:
    master = MasterData(script_id=script_id)
    now = _utcnow()
    for item in tv_users:
        username = item.get("username") or item.get("name")
        if not username:
            continue
        record = AccessRecord(
            wp_user_id=item.get("wp_user_id", ""),
            username=username,
            wp_username=item.get("wp_username", username),
            email=item.get("email", ""),
            product_id=item.get("product_id", ""),
            script_id=script_id,
            expiry=_parse_datetime(item.get("expiry")) or now,
            last_transaction_id=item.get("last_transaction_id", ""),
            last_transaction_at=_parse_datetime(
                item.get("last_transaction_at")
            )
            or now,
            status=item.get("status", "active"),
        )
        master.record_user(username, record)
    master.last_synced_at = now
    save_master(settings, master)
    return master


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


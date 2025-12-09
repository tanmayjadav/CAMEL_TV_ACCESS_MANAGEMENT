from __future__ import annotations

import argparse
import asyncio
import functools
import csv
import json
import logging
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from app import load_settings
from app.io import ApiError, TradingViewClient
from app.logic import NormalizedTransaction, normalize_transactions

BATCH_SIZE = 500
LOGGER = logging.getLogger("batch_grant")


def _chunked(seq: Sequence, size: int) -> Iterator[Sequence]:
    for start in range(0, len(seq), size):
        yield seq[start : start + size]

# python batch_grant.py --transactions "https://camelfinance.co.uk/wp-json/memberpress/v1/all-user-transactions" --csv Others/tv_users_full.csv --batch-size 500 --max-batches 1
async def _fetch_transactions_source(
    source: Union[Path, str],
    settings,
) -> List[NormalizedTransaction]:
    path = Path(source) if not isinstance(source, Path) else source
    raw_text = path.read_text(encoding="utf-8")
    raw = json.loads(raw_text)
    
    '''    if isinstance(source, Path):
        raw_text = source.read_text(encoding="utf-8")
        raw = json.loads(raw_text)
    else:
        params = {}
        if settings.wordpress.api_token_param and settings.wordpress.api_token:
            params[settings.wordpress.api_token_param] = settings.wordpress.api_token
        async with httpx.AsyncClient(timeout=settings.wordpress.timeout_seconds) as client:
            response = await client.get(source, params=params)
            response.raise_for_status()
            raw = response.json()'''
    if not isinstance(raw, list):
        raise RuntimeError(
            f"Expected a list of transactions, got {type(raw).__name__} from {source}"
        )
    return normalize_transactions(raw, settings)


def _load_existing_usernames(csv_path: Path) -> Tuple[set[str], List[List[str]]]:
    usernames: set[str] = set()
    rows: List[List[str]] = []
    if not csv_path.exists():
        return usernames, rows
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header:
            rows.append(header)
        for row in reader:
            rows.append(row)
            if len(row) > 1 and row[1]:
                usernames.add(row[1].lower())
    return usernames, rows


def _append_csv_row(
    csv_path: Path,
    header: List[str],
    row: List[str],
) -> None:
    file_exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


def _extract_csv_row(
    txn: NormalizedTransaction,
    validation: dict,
) -> List[str]:
    suggestion = None
    suggestions = validation.get("allUserSuggestions") or []
    effective_username = validation.get("verifiedUserName") or txn.username
    for candidate in suggestions:
        if candidate.get("username", "").lower() == effective_username.lower():
            suggestion = candidate
            break
    if suggestion is None and suggestions:
        suggestion = suggestions[0]

    user_id = ""
    userpic = ""
    created = ""
    expiration = txn.computed_expiry.isoformat()
    if suggestion:
        user_id = str(suggestion.get("id", "") or "")
        userpic = suggestion.get("userpic", "") or ""

    created = txn.created_at.isoformat()
    fetched_at = datetime.now(tz=timezone.utc).isoformat()
    return [user_id, effective_username, userpic, created, expiration, fetched_at]


async def process_batch(
    batch: Sequence[NormalizedTransaction],
    tv_client: TradingViewClient,
    existing_usernames: set[str],
    csv_path: Path,
    csv_header: List[str],
    summary: dict,
    dry_run: bool,
) -> None:
    for txn in batch:
        try:
            validation = await tv_client.validate_username(txn.username)
        except Exception as exc:  # pragma: no cover - network
            LOGGER.exception(
                "Validation request failed",
                extra={"transactionId": txn.transaction_id, "username": txn.username},
            )
            summary["validation_failed"] += 1
            continue

        if not validation.get("validUser"):
            LOGGER.warning(
                "Username invalid",
                extra={"transactionId": txn.transaction_id, "username": txn.username},
            )
            summary["invalid_usernames"] += 1
            continue

        effective_username = validation.get("verifiedUserName") or txn.username
        payload = {
            "scriptId": txn.script_id,
            "username": effective_username,
            "email": txn.email,
            "expiry": txn.computed_expiry.date().isoformat(),
            "subscription_type": txn.subscription_type or "",
            "wp_username": txn.wp_username,
            "remarks": txn.remarks or "paid",
        }

        if dry_run:
            LOGGER.info(
                "Dry run: would grant access",
                extra={
                    "username": effective_username,
                    "transactionId": txn.transaction_id,
                    "payload": payload,
                },
            )
            summary["dry_run_skipped"] += 1
            continue

        try:
            await tv_client.grant_access(payload)
        except ApiError as exc:
            LOGGER.error(
                "Grant access failed",
                extra={
                    "transactionId": txn.transaction_id,
                    "username": effective_username,
                    "statusCode": exc.status_code,
                },
            )
            summary["grant_failed"] += 1
            continue

        username_key = effective_username.lower()
        if username_key in existing_usernames:
            LOGGER.info(
                "Access refreshed for existing user",
                extra={"username": effective_username, "transactionId": txn.transaction_id},
            )
            summary["refreshed"] += 1
        else:
            row = _extract_csv_row(txn, validation)
            _append_csv_row(csv_path, csv_header, row)
            existing_usernames.add(username_key)
            LOGGER.info(
                "Access granted to new user",
                extra={"username": effective_username, "transactionId": txn.transaction_id},
            )
            summary["new_grants"] += 1


async def main(
    transactions_source: Union[Path, str],
    csv_path: Path,
    batch_size: int = BATCH_SIZE,
    max_batches: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    settings = load_settings()
    transactions = await _fetch_transactions_source(transactions_source, settings)
    existing_usernames, existing_rows = _load_existing_usernames(csv_path)

    if existing_rows:
        header = existing_rows[0]
    else:
        header = ["id", "username", "userpic", "created", "expiration", "fetched_at"]

    tv_client = TradingViewClient(settings)
    summary = {
        "total_transactions": len(transactions),
        "processed_batches": 0,
        "new_grants": 0,
        "refreshed": 0,
        "invalid_usernames": 0,
        "grant_failed": 0,
        "validation_failed": 0,
        "dry_run_skipped": 0,
    }

    for batch_index, batch in enumerate(_chunked(transactions, batch_size), start=1):
        await process_batch(
            batch,
            tv_client,
            existing_usernames,
            csv_path,
            header,
            summary,
            dry_run,
        )
        summary["processed_batches"] += 1
        if max_batches is not None and batch_index >= max_batches:
            break

    LOGGER.info("Batch processing completed", extra={"extra_data": summary})
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch grant TradingView access from transactions JSON")
    parser.add_argument(
        "--transactions",
        type=str,
        default="C:\\Tanmay Jadav\\Variance\\inviteOnlyScript\\Others\\transactions.json",
        help="Path to the transactions JSON file",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("Others/tv_users_full.csv"),
        help="Path to the CSV tracking current TradingView users",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Number of transactions per batch (default: 500)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of batches to process this run (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be granted without calling the TradingView API or updating the CSV",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    transactions_source: Union[Path, str]
    try:
        transactions_source = Path(args.transactions)
        if not transactions_source.exists():
            transactions_source = args.transactions
    except OSError:
        transactions_source = args.transactions

    asyncio.run(
        main(
            transactions_source,
            args.csv,
            args.batch_size,
            args.max_batches,
            args.dry_run,
        )
    )

'''Core processing (process_batch 500)

For each transaction in the batch:

1. Call TradingViewClient.validate_username.
    - If it fails or validUser is false, log and skip.
2. Use the verified username (falls back to original).
3. Build the TradingView grant payload:
    - scriptId, username, email, expiry (date), subscription type, WordPress username, remarks.
4. Attempt grant_access.
    - On failure, log and increment summary counters.
5. Check if the username already exists in the CSV set:
    - If yes, treat it as a refresh; log but don’t append.
    - If no, append a new CSV row (using _extract_csv_row), add to the set, and log as a new grant.'''
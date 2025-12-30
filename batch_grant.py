from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import httpx

from app import load_settings
from app.io import ApiError, TradingViewClient
from app.storage import AccessRecord, MasterData, load_master, save_master

BATCH_SIZE = 500
LOGGER = logging.getLogger("batch_grant")


def _chunked(seq: list, size: int):
    """Split sequence into chunks of specified size"""
    for start in range(0, len(seq), size):
        yield seq[start : start + size]


def parse_expiry(expiry_str: str) -> Optional[str]:
    """Parse expiry date string and return ISO format date string"""
    if not expiry_str:
        return None
    try:
        # Handle various date formats
        if "T" in expiry_str or " " in expiry_str:
            dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
            return dt.date().isoformat()
        dt = datetime.strptime(expiry_str, "%Y-%m-%d")
        return dt.date().isoformat()
    except:
        return None


def _parse_expiry_to_datetime(expiry_str: str) -> Optional[datetime]:
    """Parse expiry string to datetime object"""
    if not expiry_str:
        return None
    try:
        if "T" in expiry_str or " " in expiry_str:
            dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(expiry_str, "%Y-%m-%d")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


async def _fetch_transactions_source(
    source: Union[Path, str],
    settings,
) -> List[Dict[str, Any]]:
    """Fetch raw transactions from API or file"""
    if isinstance(source, Path):
        raw_text = source.read_text(encoding="utf-8")
        raw = json.loads(raw_text)
    else:
        params = {}
        if settings.wordpress.api_token_param and settings.wordpress.api_token:
            params[settings.wordpress.api_token_param] = settings.wordpress.api_token
        async with httpx.AsyncClient(timeout=settings.wordpress.timeout_seconds) as client:
            response = await client.get(source, params=params)
            response.raise_for_status()
            raw = response.json()
    
    if not isinstance(raw, list):
        raise RuntimeError(
            f"Expected a list of transactions, got {type(raw).__name__} from {source}"
        )
    return raw


def _build_transaction_lookup(raw_transactions: List[Dict]) -> Dict[str, Dict]:
    """Build lookup from raw transactions (like compare_access.py) - no status filtering"""
    lookup: Dict[str, Dict] = {}
    
    for txn in raw_transactions:
        # Get email and expiry
        email = txn.get("user_email", "").strip()
        expires_at = txn.get("expires_at", "").strip()
        
        # Parse expiry to date string
        expiry_date = parse_expiry(expires_at) if expires_at else None
        
        # Get possible usernames to match against
        user_meta = txn.get("user_meta", {})
        tv_username_meta = user_meta.get("tradingview_username", "").strip() if user_meta else ""
        user_login = txn.get("user_login", "").strip()
        user_id = txn.get("user_id", "") or (txn.get("user", {}).get("id", "") if txn.get("user") else "")
        transaction_id = txn.get("transaction_id", "")
        created_at = txn.get("created_at", "")
        product_id = txn.get("product_id", "")
        
        # Transaction data to store
        txn_data = {
            'email': email,
            'expiry': expiry_date,
            'transaction_id': transaction_id,
            'created_at': created_at,
            'product_id': product_id,
            'user_id': str(user_id) if user_id else "",
            'user_login': user_login,
        }
        
        # Add to lookup by tradingview_username (if exists)
        if tv_username_meta:
            username_lower = tv_username_meta.lower()
            existing = lookup.get(username_lower)
            
            if not existing:
                lookup[username_lower] = txn_data.copy()
            else:
                # Update if this transaction has a later expiry
                if expiry_date and existing['expiry']:
                    try:
                        existing_date = datetime.fromisoformat(existing['expiry']).date()
                        new_date = datetime.fromisoformat(expiry_date).date()
                        if new_date > existing_date:
                            lookup[username_lower] = txn_data.copy()
                    except:
                        pass
                elif expiry_date and not existing['expiry']:
                    lookup[username_lower] = txn_data.copy()
                elif not existing['email'] and email:
                    lookup[username_lower] = txn_data.copy()
    
    return lookup


def _load_csv_users(csv_path: Path) -> List[Dict[str, str]]:
    """Load CSV users and return list of user dicts"""
    users = []
    if not csv_path.exists():
        return users
    
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            username = row.get("username", "").strip()
            if username:
                users.append({"username": username})
    return users


def _load_grant_csv(grant_csv_path: Path) -> set[str]:
    """Load grant CSV and return set of usernames already processed"""
    usernames = set()
    if not grant_csv_path.exists():
        return usernames
    
    with grant_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            username = row.get("TV_username", "").strip()
            if username:
                usernames.add(username.lower())
    return usernames


def _append_grant_csv(
    grant_csv_path: Path,
    username: str,
    email: str,
    expiry: str,
    transaction_id: str,
    last_payment: str,
    user_login: str,
) -> None:
    """Append user to grant CSV"""
    file_exists = grant_csv_path.exists()
    header = ["TV_username", "email", "expiry", "transaction_id", "last_payment", "user_login"]
    row = [username, email, expiry, transaction_id, last_payment, user_login]
    
    with grant_csv_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


def _get_default_script_id(settings) -> str:
    """Get default script_id (first product in config)"""
    if settings.products:
        first_product_id = next(iter(settings.products.keys()))
        product = settings.products[first_product_id]
        return product.script_id
    raise RuntimeError("No products configured in settings")


async def process_user_batch(
    batch: List[Dict[str, str]],
    transaction_lookup: Dict[str, Dict],
    tv_client: TradingViewClient,
    settings,
    master_data: MasterData,
    grant_csv_path: Path,
    grant_csv_usernames: set[str],
    summary: dict,
    dry_run: bool,
) -> None:
    """Process a batch of CSV users"""
    now = datetime.now(tz=timezone.utc)
    default_script_id = _get_default_script_id(settings)
    separator_line = "*" * 100
    
    # First pass: collect active users and track skipped ones
    active_users_in_batch = []
    for csv_user in batch:
        tv_username = csv_user["username"]
        username_key = tv_username.lower()
        
        # Check if already processed
        if username_key in grant_csv_usernames or username_key in master_data.users:
            summary["already_processed"] += 1
            continue
        
        # Match to transaction lookup
        txn_info = transaction_lookup.get(username_key)
        if not txn_info:
            summary["no_transaction_match"] += 1
            continue
        
        # Get expiry from transaction
        expiry_date_str = txn_info.get('expiry')
        if not expiry_date_str:
            summary["no_expiry"] += 1
            continue
        
        # Parse expiry and check if expired
        expiry_dt = _parse_expiry_to_datetime(expiry_date_str)
        if not expiry_dt:
            summary["invalid_expiry"] += 1
            continue
        
        if expiry_dt < now:
            # Expired - add to CSV but skip grant
            email = txn_info.get('email', '')
            transaction_id = txn_info.get('transaction_id', '')
            created_at = txn_info.get('created_at', '')
            user_login = txn_info.get('user_login', '')
            
            # Append expired user to grant CSV
            _append_grant_csv(
                grant_csv_path,
                tv_username,
                email,
                expiry_date_str,
                transaction_id,
                created_at,
                user_login,
            )
            summary["expired_skipped"] += 1
            grant_csv_usernames.add(username_key)  # Mark as processed to avoid duplicates
            continue
        
        # Active user - will be processed
        active_users_in_batch.append((csv_user, txn_info, expiry_dt))
    
    # Second pass: process active users with detailed logging
    total_in_batch = len(active_users_in_batch)
    for index, (csv_user, txn_info, expiry_dt) in enumerate(active_users_in_batch, start=1):
        tv_username = csv_user["username"]
        username_key = tv_username.lower()
        
        LOGGER.info(separator_line)
        LOGGER.info(f"Processing user {tv_username} ({index}/{total_in_batch})")
        
        # Get user details
        email = txn_info.get('email', '')
        transaction_id = txn_info.get('transaction_id', '')
        created_at = txn_info.get('created_at', '')
        user_login = txn_info.get('user_login', '')
        user_id = txn_info.get('user_id', '')
        product_id = txn_info.get('product_id', '')
        expiry_date_str = txn_info.get('expiry', '')
        
        # Get script_id and subscription_type
        product = settings.product_for(product_id) if product_id else None
        script_id = product.script_id if product else default_script_id
        subscription_type = product.subscription_type if product else ""
        
        LOGGER.info(
            "TV username=%s email=%s expiry=%s product_id=%s script_id=%s",
            tv_username,
            email,
            expiry_date_str,
            product_id or "unknown",
            script_id,
        )
        
        # Validate username (skip in dry-run)
        effective_username = tv_username
        if not dry_run:
            try:
                validation = await tv_client.validate_username(tv_username)
            except Exception as exc:
                LOGGER.exception(
                    "Validation request failed",
                    extra={"username": tv_username, "transactionId": transaction_id},
                )
                summary["validation_failed"] += 1
                continue
            
            if not validation.get("validUser"):
                LOGGER.warning(
                    "Username invalid",
                    extra={"username": tv_username, "transactionId": transaction_id},
                )
                summary["invalid_usernames"] += 1
                continue
            
            effective_username = validation.get("verifiedUserName") or tv_username
        
        # Build grant payload
        payload = {
            "scriptId": script_id,
            "username": effective_username,
            "email": email,
            "expiry": expiry_date_str,  # Already in YYYY-MM-DD format
            "subscription_type": subscription_type,
            "wp_username": user_login,
            "remarks": "OP-M",
        }
        
        if dry_run:
            # Dry-run: log what would be granted
            action_type = "grant_new" if username_key not in master_data.users else "update_existing"
            LOGGER.info(f"Dry run: would call TradingView {action_type} for {effective_username}")
            summary["dry_run_skipped"] += 1
            continue
        
        # Grant access
        try:
            await tv_client.grant_access(payload)
            action_type = "grant_new" if username_key not in master_data.users else "update_existing"
            LOGGER.info(f"Successfully called TradingView {action_type} for {effective_username}")
        except ApiError as exc:
            LOGGER.error(
                "Grant access failed for %s (status: %s)",
                effective_username,
                exc.status_code,
            )
            summary["grant_failed"] += 1
            continue
        
        # Check if this is a refresh or new grant
        is_refresh = username_key in master_data.users
        
        # Update masterData
        expiry_datetime = expiry_dt
        created_at_dt = _parse_expiry_to_datetime(created_at) or now
        
        access_record = AccessRecord(
            wp_user_id=user_id or effective_username,
            username=effective_username,
            wp_username=user_login or effective_username,
            email=email,
            product_id=product_id or "unknown",
            script_id=script_id,
            expiry=expiry_datetime,
            last_transaction_id=transaction_id,
            last_transaction_at=created_at_dt,
            status="active",
        )
        master_data.record_user(effective_username.lower(), access_record)
        
        # Append to grant CSV
        _append_grant_csv(
            grant_csv_path,
            effective_username,
            email,
            expiry_date_str,
            transaction_id,
            created_at,
            user_login or user_id,
        )
        
        # Update summary
        if is_refresh:
            summary["refreshed"] += 1
        else:
            summary["new_grants"] += 1
        
        summary["active_granted"] += 1
        grant_csv_usernames.add(username_key)  # Mark as processed


async def main(
    transactions_source: Union[Path, str],
    csv_path: Path,
    batch_size: int = BATCH_SIZE,
    max_batches: Optional[int] = None,
    dry_run: bool = False,
    grant_csv_path: Optional[Path] = None,
) -> None:
    settings = load_settings()
    
    # Fetch raw transactions
    LOGGER.info("Fetching transactions from source...")
    raw_transactions = await _fetch_transactions_source(transactions_source, settings)
    LOGGER.info(f"Fetched {len(raw_transactions)} transactions")
    
    # Build transaction lookup (like compare_access.py)
    LOGGER.info("Building transaction lookup...")
    transaction_lookup = _build_transaction_lookup(raw_transactions)
    LOGGER.info(f"Created lookup for {len(transaction_lookup)} unique usernames")
    
    # Load CSV users
    LOGGER.info(f"Loading CSV users from {csv_path}...")
    csv_users = _load_csv_users(csv_path)
    LOGGER.info(f"Loaded {len(csv_users)} users from CSV")
    
    # Setup grant CSV
    if grant_csv_path is None:
        grant_csv_path = csv_path.parent / "granted_users.csv"
    grant_csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load already processed usernames from grant CSV
    grant_csv_usernames = _load_grant_csv(grant_csv_path)
    LOGGER.info(f"Found {len(grant_csv_usernames)} users already in grant CSV")
    
    default_script_id = _get_default_script_id(settings)
    master_data = load_master(settings, default_script_id)
    LOGGER.info(f"Loaded masterData with {len(master_data.users)} existing users")
    
    # Initialize summary
    summary = {
        "total_csv_users": len(csv_users),
        "processed_batches": 0,
        "already_processed": 0,
        "no_transaction_match": 0,
        "no_expiry": 0,
        "invalid_expiry": 0,
        "expired_skipped": 0,
        "validation_failed": 0,
        "invalid_usernames": 0,
        "grant_failed": 0,
        "dry_run_skipped": 0,
        "active_granted": 0,
        "new_grants": 0,
        "refreshed": 0,
    }
    
    # Count expired users in CSV (for summary)
    now = datetime.now(tz=timezone.utc)
    expired_in_csv = 0
    active_in_csv = 0
    
    for csv_user in csv_users:
        username_key = csv_user["username"].lower()
        txn_info = transaction_lookup.get(username_key)
        
        if txn_info:
            expiry_str = txn_info.get('expiry')
            if expiry_str:
                expiry_dt = _parse_expiry_to_datetime(expiry_str)
                if expiry_dt:
                    if expiry_dt < now:
                        expired_in_csv += 1
                    else:
                        active_in_csv += 1
    
    summary["expired_in_csv"] = expired_in_csv
    summary["active_in_csv"] = active_in_csv
    
    # Count total active users for progress tracking
    total_active_users = 0
    for csv_user in csv_users:
        username_key = csv_user["username"].lower()
        if username_key in grant_csv_usernames or username_key in master_data.users:
            continue
        txn_info = transaction_lookup.get(username_key)
        if txn_info:
            expiry_str = txn_info.get('expiry')
            if expiry_str:
                expiry_dt = _parse_expiry_to_datetime(expiry_str)
                if expiry_dt and expiry_dt >= now:
                    total_active_users += 1
    
    summary["total_active_users"] = total_active_users
    LOGGER.info(f"Total active users to process: {total_active_users}")
    
    # Process users in batches
    for batch_index, batch in enumerate(_chunked(csv_users, batch_size), start=1):
        LOGGER.info(f"Processing batch {batch_index} ({len(batch)} users)...")
        
        await process_user_batch(
            batch,
            transaction_lookup,
            TradingViewClient(settings),
            settings,
            master_data,
            grant_csv_path,
            grant_csv_usernames,
            summary,
            dry_run,
        )
        
        # Save masterData after each batch
        if not dry_run:
            save_master(settings, master_data)
            LOGGER.info(f"Saved masterData after batch {batch_index}")
        
        summary["processed_batches"] += 1
        
        if max_batches is not None and batch_index >= max_batches:
            break
    
    # Print summary
    LOGGER.info("Batch processing completed", extra={"extra_data": summary})
    
    print("\n" + "=" * 80)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Total CSV users: {summary['total_csv_users']}")
    print(f"Processed batches: {summary['processed_batches']}")
    print(f"\nExpired users (TV_username still present in CSV): {summary['expired_in_csv']}")
    print(f"Active users in CSV: {summary['active_in_csv']}")
    print(f"\nProcessing results:")
    print(f"  - Already processed (skipped): {summary['already_processed']}")
    print(f"  - No transaction match: {summary['no_transaction_match']}")
    print(f"  - No expiry: {summary['no_expiry']}")
    print(f"  - Invalid expiry: {summary['invalid_expiry']}")
    print(f"  - Expired skipped: {summary['expired_skipped']}")
    print(f"  - Validation failed: {summary['validation_failed']}")
    print(f"  - Invalid usernames: {summary['invalid_usernames']}")
    print(f"  - Grant failed: {summary['grant_failed']}")
    if dry_run:
        print(f"  - Dry run skipped: {summary['dry_run_skipped']}")
    print(f"\nGrants:")
    print(f"  - Active users granted: {summary['active_granted']}")
    print(f"  - New grants: {summary['new_grants']}")
    print(f"  - Refreshed: {summary['refreshed']}")
    print("=" * 80 + "\n")
    
    # Also print JSON for programmatic use
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch grant TradingView access from CSV users")
    parser.add_argument(
        "--transactions",
        type=str,
        default="https://camelfinance.co.uk/wp-json/memberpress/v1/all-user-transactions?api_token=camelfinance_api_2025",
        help="URL to fetch transactions from WordPress API (or local file path as fallback)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("tv_users_full.csv"),
        help="Path to the CSV tracking current TradingView users",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Number of users per batch (default: 500)",
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
        help="Log what would be granted without calling the TradingView API or updating files",
    )
    parser.add_argument(
        "--grant-csv",
        type=Path,
        default=None,
        help="Path to CSV file for granted users (default: granted_users.csv in same directory as --csv)",
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
            args.grant_csv,
        )
    )

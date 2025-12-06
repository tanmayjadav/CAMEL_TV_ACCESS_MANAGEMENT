# Access Management Sync

Automated grant/stack workflow that syncs WordPress MemberPress transactions with the TradingView access API. It follows the access logic in `AlgoDOC.txt`, keeps state in per-script JSON files, and exposes both a FastAPI endpoint and an optional scheduler entrypoint.

## Requirements

- Python 3.10+
- `pip install -r requirements.txt` (see below for suggested dependencies)

Suggested core packages:

- `fastapi`
- `uvicorn`
- `httpx`
- `pydantic`
- `apscheduler`
- `pytest` (for tests)

## Configuration

Update `config.json` with project-specific values:

- `wordpress`: Base URL, transactions endpoint (`/all-user-transactions` by default), optional since parameter key (expects UNIX seconds), auth, status filter, timeout.
- `tradingview`: Base URL, grant/update/list/validate endpoints, API key header/value, retries.
- `products`: Product → TradingView script ID mapping, plan duration, stacking policy.
- `scheduler`: Interval (minutes) for automatic sync.
- `paths`: Directories for master data JSON and log output.
- `email`: SMTP credentials for outbound notifications (invalid usernames, etc.).

All fields are validated via Pydantic during startup. Logs are emitted in JSON format to stdout and to `logs/sync.log`.

## Master Data

The first successful sync creates `masterData/{scriptId}.json`. Each file tracks:

- Known users, their latest expiry, and grant history.
- Recently processed transaction IDs (rolling window of 500).
- Retry queue and manual review entries.

A bootstrap run pulls existing TradingView users to seed the file when empty.

## Running the API

```bash
uvicorn app.main:app --reload
```

Or launch via the helper script:

```bash
python launch.py api --host 0.0.0.0 --port 8000
```

Or use the helper batch scripts on Windows:

- `setup.bat` — create `.venv`, upgrade pip, and install `requirements.txt`.
- `run.bat` — activate `.venv` and start the FastAPI server on port 8000.

- `GET /health` — readiness probe.
- `POST /sync` — triggers a sync via `run_sync`.

## Scheduler

Use the APScheduler runner when you need interval-based execution:

```bash
python -m app.scheduler
```

Or with the launcher:

```bash
python launch.py scheduler
```

The scheduler respects `scheduler.interval_minutes` from `config.json`.
Set `scheduler.dry_run` to `true` in `config.json` to have the scheduler log intended TradingView calls without executing them.

## Testing

```bash
pytest
```

To trigger a single sync run for debugging:

```bash
python launch.py sync
```

Included tests cover normalization logic, master data persistence, and the main sync flow using mocked clients.
Append `?dry_run=true` to `POST /sync` (or use `python launch.py sync -- --dry-run` via FastAPI query) when you want a log-only run.

## Notes

- Re-run `/sync` or the scheduler safely: processed transactions are idempotent.
- WordPress fetches use the earliest per-script `last_processed_at` marker as the `since` parameter (UNIX epoch seconds), so only new transactions are pulled.
- Before calling TradingView, the system validates each username via `GET {validate_endpoint}`. Invalid usernames trigger an email to the customer using `templates/invalid_username.html` and move the transaction to manual review until the username is corrected.
- New users trigger TradingView `POST /tradingview/access/grant`; existing users with stacking enabled call `POST /tradingview/access/update` to extend expiry. Payloads include `scriptId`, `username`, `email`, `expiry`, `subscription_type`, `wp_username`, and `remarks`.

## Manual Grant Test

To send a single grant request against the configured TradingView endpoint (useful for manual verification with real credentials):

```bash
python manual_grant_test.py
```

The script uses the sample transaction embedded inside it; edit `manual_grant_test.py` to customise the payload if needed. Ensure `config.json` contains the correct TradingView API key before running.

## Email Notifications

Populate the `email` section in `config.json` to enable SMTP alerts. The sync job will send an "invalid username" email (based on `app/templates/invalid_username.html`) whenever TradingView does not recognise a username returned by WordPress. Leave the block empty to disable email sending.

## Batch Grant Utility

To process a JSON dump of transactions in bulk, validate TradingView usernames, and sync them with the existing access CSV, run:

```bash
python batch_grant.py --transactions Others/transactions.json --csv Others/tv_users_full.csv --batch-size 500
```

The script will:
- Validate each username via the TradingView `validate_endpoint`.
- Skip invalid usernames (logged as manual review required).
- Refresh access for users already present in the CSV.
- Grant access and append new rows to the CSV for users not yet present.
- Process the workload in batches (default 500 per run). Use `--max-batches 1` if you only want to process a single batch on the current run (e.g., 500 users today, the next 500 later).
- Add `--dry-run` to log the payloads without calling TradingView or updating the CSV.
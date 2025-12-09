from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Optional

try:
    import uvicorn  # type: ignore
except ImportError:  # pragma: no cover
    uvicorn = None

from app import configure_logging, load_settings, run_sync
from app.scheduler import start_scheduler


def _run_api(host: str, port: int, reload: bool) -> None:
    if uvicorn is None:
        raise SystemExit("uvicorn is not installed. Run setup.bat or pip install -r requirements.txt")
    uvicorn.run("app.main:app", host=host, port=port, reload=reload)


def _run_scheduler(config_path: Optional[str] = None) -> None:
    asyncio.run(start_scheduler(config_path))


async def _run_once(config_path: Optional[str], dry_run: bool) -> None:
    settings = load_settings(config_path)
    if dry_run:
        settings.scheduler.dry_run = True
    await run_sync(settings)
    

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch utilities for the Access Management Sync system.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.json (defaults to ./config.json)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        help="Override logging level (INFO, DEBUG, etc.)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log-only mode (no TradingView mutations) for sync command",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    api_parser = subparsers.add_parser("api", help="Run FastAPI server with uvicorn")
    api_parser.add_argument("--host", default="0.0.0.0")
    api_parser.add_argument("--port", type=int, default=8000)
    api_parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload (development only)",
    )

    subparsers.add_parser(
        "scheduler",
        help="Run the APScheduler loop using interval from config.json",
    )

    subparsers.add_parser(
        "sync",
        help="Execute a single sync run immediately and exit",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(args.config)

    if args.log_level:
        configure_logging(args.log_level, settings.logs_path)

    logging.getLogger(__name__).info("launch.command", extra={"extra_data": {"command": args.command}})

    if args.command == "api":
        _run_api(args.host, args.port, args.reload)
    elif args.command == "scheduler":
        _run_scheduler(args.config)
    elif args.command == "sync":
        asyncio.run(_run_once(args.config, args.dry_run))
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()


from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .config import get_settings
from .sync import run_sync
from .storage import load_master

logger = logging.getLogger(__name__)


async def _check_sync_health(settings) -> None:
    """Check if sync hasn't run for more than 1 hour and send Discord alert."""
    try:
        script_ids = {product.script_id for product in settings.products.values()}
        now = datetime.now(tz=timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        
        all_stale = True
        oldest_sync = None
        
        for script_id in script_ids:
            try:
                master = load_master(settings, script_id)
                if master.last_synced_at:
                    if master.last_synced_at > one_hour_ago:
                        all_stale = False
                        break
                    if oldest_sync is None or master.last_synced_at < oldest_sync:
                        oldest_sync = master.last_synced_at
            except Exception:
                continue
        
        if all_stale and oldest_sync:
            hours_since = (now - oldest_sync).total_seconds() / 3600
            logger.warning(
                "scheduler.sync_stale",
                extra={
                    "extra_data": {
                        "hours_since_last_sync": round(hours_since, 2),
                        "last_sync": oldest_sync.isoformat(),
                    }
                },
            )
            # Send Discord alert
            try:
                from .discord_alert import send_discord_alert_if_enabled
                send_discord_alert_if_enabled(
                    settings,
                    message={
                        "title": "Sync Job Stale",
                        "description": f"Sync job has not run for {round(hours_since, 1)} hours",
                        "last_sync": oldest_sync.isoformat(),
                        "threshold": "1 hour",
                        "color": "red"
                    }
                )
            except Exception:
                pass  # Don't let Discord failures break monitoring
    except Exception as e:
        logger.error("scheduler.health_check_failed", extra={"extra_data": {"error": str(e)}})


async def start_scheduler(config_path: Optional[str] = None, dry_run: Optional[bool] = None) -> None:
    settings = get_settings(config_path)
    if dry_run is not None:
        settings.scheduler.dry_run = dry_run
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        run_sync,
        "interval",
        minutes=settings.scheduler.interval_minutes,
        kwargs={"settings": settings},
        id="access_sync",
        max_instances=1,
        coalesce=True,
    )
    
    # Add health check job - runs every 30 minutes to check if sync is stale
    scheduler.add_job(
        _check_sync_health,
        "interval",
        minutes=60,
        kwargs={"settings": settings},
        id="sync_health_check",
        max_instances=1,
    )

    scheduler.start()
    logger.info(
        "scheduler.started",
        extra={
            "extra_data": {
                "intervalMinutes": settings.scheduler.interval_minutes
            }
        },
    )

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("scheduler.stopping")
    finally:
        scheduler.shutdown()
        logger.info("scheduler.stopped")


def run() -> None:
    asyncio.run(start_scheduler())


from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .config import get_settings
from .sync import run_sync

logger = logging.getLogger(__name__)


async def start_scheduler() -> None:
    settings = get_settings()
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        run_sync,
        "interval",
        minutes=settings.scheduler.interval_minutes,
        kwargs={"settings": settings, "dry_run": settings.scheduler.dry_run},
        id="access_sync",
        max_instances=1,
        coalesce=True,
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


from __future__ import annotations

from fastapi import Depends, FastAPI

from .config import Settings, get_settings
from .sync import run_sync

app = FastAPI(title="Access Management Sync")


@app.get("/health")
async def health_check() -> dict:
    return {"status": "ok"}


@app.post("/sync")
async def trigger_sync(dry_run: bool = False, settings: Settings = Depends(get_settings)) -> dict:
    if dry_run:
        settings.scheduler.dry_run = True
    return await run_sync(settings)


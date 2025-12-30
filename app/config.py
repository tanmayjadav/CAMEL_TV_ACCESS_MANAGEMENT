from __future__ import annotations

import json
import logging
import pathlib
from functools import lru_cache
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class WordPressConfig(BaseModel):
    base_url: HttpUrl
    transactions_endpoint: str = "/users/transactions"
    since_param: str = "since"
    transactions_limit: Optional[int] = Field(default=None, ge=1)
    status_filter: List[str] = Field(default_factory=lambda: ["complete", "confirmed"])
    timeout_seconds: int = Field(default=30, ge=1)
    api_key: Optional[str] = None
    basic_auth_user: Optional[str] = None
    basic_auth_password: Optional[str] = None
    api_token_param: Optional[str] = None
    api_token: Optional[str] = None


class TradingViewConfig(BaseModel):
    base_url: HttpUrl
    grant_endpoint: str = "/tradingview/access/grant"
    update_endpoint: str = "/tradingview/access/update"
    list_users_endpoint: str = "/tradingview/access/scriptUsers/{scriptId}"
    validate_endpoint: str = "/tradingview/validate/{username}"
    api_key_header: str = "x-api-key"
    api_key: str
    timeout_seconds: int = Field(default=30, ge=1)
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_seconds: List[int] = Field(
        default_factory=lambda: [5, 15, 60]
    )


class ProductConfig(BaseModel):
    script_id: str
    duration_days: int = Field(ge=1)
    subscription_type: Optional[str] = None
    stacking_allowed: bool = True


class EmailConfig(BaseModel):
    smtp_server: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    from_email: str
    bcc: Optional[List[str]] = None


class SchedulerConfig(BaseModel):
    interval_minutes: int = Field(default=15, ge=1)
    dry_run: bool = False

class LoggingConfig(BaseModel):
    level: str = Field(default="INFO")


class PathConfig(BaseModel):
    masterdata_dir: str = "masterData"
    logs_dir: str = "logs"


class DiscordConfig(BaseModel):
    webhook_url: Optional[str] = None
    author: str = "Access Sync Bot"
    enabled: bool = True


class Settings(BaseModel):
    wordpress: WordPressConfig
    tradingview: TradingViewConfig
    products: Dict[str, ProductConfig]
    scheduler: SchedulerConfig = SchedulerConfig()
    logging: LoggingConfig = LoggingConfig()
    paths: PathConfig = PathConfig()
    email: Optional[EmailConfig] = None
    discord: Optional[DiscordConfig] = None

    def product_for(self, product_id: str) -> Optional[ProductConfig]:
        return self.products.get(str(product_id))

    @property
    def masterdata_path(self) -> pathlib.Path:
        return pathlib.Path(self.paths.masterdata_dir)

    @property
    def logs_path(self) -> pathlib.Path:
        return pathlib.Path(self.paths.logs_dir)


def _load_settings_from_file(path: pathlib.Path) -> Settings:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    settings = Settings.model_validate(raw)
    return settings


def load_settings(config_path: Optional[str] = None) -> Settings:
    path = pathlib.Path(config_path or "config.json")
    settings = _load_settings_from_file(path)
    configure_logging(settings.logging.level, settings.logs_path)
    return settings


def configure_logging(level: str, logs_dir: Optional[pathlib.Path] = None) -> None:
    log_format = "%(asctime)s %(levelname)s %(name)s : %(message)s"

    class PlainFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            message = super().format(record)
            extra = record.__dict__.get("extra_data")
            if extra:
                message = f"{message} {extra}"
            return message

    logging_level = getattr(logging, level.upper(), logging.INFO)
    handlers: List[logging.Handler] = []

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(PlainFormatter(log_format))
    handlers.append(stream_handler)

    if logs_dir:
        logs_dir.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(  # type: ignore[arg-type]
            filename=str(logs_dir / "sync.log"), when="midnight", backupCount=730, encoding="utf-8"
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(PlainFormatter(log_format))
        handlers.append(file_handler)

    logging.basicConfig(level=logging_level, handlers=handlers, force=True)


@lru_cache(maxsize=1)
def get_settings(config_path: Optional[str] = None) -> Settings:
    return load_settings(config_path)


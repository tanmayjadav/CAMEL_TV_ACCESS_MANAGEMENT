from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from .config import Settings

logger = logging.getLogger(__name__)


class ApiError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


def _join_url(base: str, endpoint: str) -> str:
    if endpoint.startswith("http"):
        return endpoint
    return f"{base.rstrip('/')}/{endpoint.lstrip('/')}"


class WordPressClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._base_url = str(settings.wordpress.base_url)
        self._endpoint = settings.wordpress.transactions_endpoint
        self._since_param = settings.wordpress.since_param
        self._timeout = settings.wordpress.timeout_seconds
        self._limit = settings.wordpress.transactions_limit
        self._api_key = settings.wordpress.api_key
        if settings.wordpress.basic_auth_user and settings.wordpress.basic_auth_password:
            self._auth = httpx.BasicAuth(
                settings.wordpress.basic_auth_user, settings.wordpress.basic_auth_password
            )
        else:
            self._auth = None

    async def fetch_transactions(self, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        url = _join_url(self._base_url, self._endpoint)
        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        params: Dict[str, Any] = {}
        if self._limit is not None:
            params["limit"] = self._limit
        if since:
            params[self._since_param] = since.astimezone(timezone.utc).isoformat()
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(
                url, params=params, headers=headers, auth=self._auth
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "wordpress.fetch_failed",
                    extra={
                        "extra_data": {
                            "status_code": exc.response.status_code,
                            "url": str(exc.request.url),
                        }
                    },
                )
                raise ApiError(
                    "WordPress transaction fetch failed",
                    status_code=exc.response.status_code,
                ) from exc

        payload = response.json()
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return data
        if isinstance(payload, list):
            return payload
        logger.warning(
            "wordpress.unexpected_response",
            extra={"extra_data": {"payload_type": type(payload).__name__}},
        )
        return []


class TradingViewClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._base_url = str(settings.tradingview.base_url)
        self._grant_endpoint = settings.tradingview.grant_endpoint
        self._update_endpoint = settings.tradingview.update_endpoint
        self._list_endpoint = settings.tradingview.list_users_endpoint
        self._validate_endpoint = settings.tradingview.validate_endpoint
        self._timeout = settings.tradingview.timeout_seconds
        self._headers = {
            settings.tradingview.api_key_header: settings.tradingview.api_key,
            "Content-Type": "application/json",
        }
        self._max_retries = settings.tradingview.max_retries
        self._backoff = settings.tradingview.retry_backoff_seconds 

    async def list_script_users(self, script_id: str) -> List[Dict[str, Any]]:
        endpoint = self._list_endpoint.replace("{scriptId}", script_id)
        url = _join_url(self._base_url, endpoint)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url, headers=self._headers)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "tradingview.list_failed",
                    extra={
                        "extra_data": {
                            "status_code": exc.response.status_code,
                            "scriptId": script_id,
                        }
                    },
                )
                raise ApiError(
                    "TradingView list users failed",
                    status_code=exc.response.status_code,
                ) from exc
        payload = response.json()
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return data
        if isinstance(payload, list):
            return payload
        logger.warning(
            "tradingview.unexpected_response",
            extra={"extra_data": {"payload_type": type(payload).__name__}},
        )
        return []

    async def validate_username(self, username: str) -> Dict[str, Any]:
        endpoint = self._validate_endpoint.replace("{username}", username)
        url = _join_url(self._base_url, endpoint)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                response = await client.get(url, headers=self._headers)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "tradingview.validate_failed",
                    extra={
                        "extra_data": {
                            "status_code": exc.response.status_code,
                            "username": username,
                        }
                    },
                )
                raise ApiError(
                    "TradingView validate username failed",
                    status_code=exc.response.status_code,
                ) from exc
            except httpx.HTTPError as exc:
                logger.error(
                    "tradingview.validate_transport_error",
                    extra={
                        "extra_data": {
                            "username": username,
                            "error": str(exc),
                        }
                    },
                )
                raise ApiError("TradingView validate username failed") from exc
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        logger.warning(
            "tradingview.unexpected_validation_response",
            extra={"extra_data": {"payload_type": type(payload).__name__}},
        )
        raise ApiError("Unexpected TradingView validation response")

    async def grant_access(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._post_with_retry(
            endpoint=self._grant_endpoint,
            payload=payload,
            success_event="tradingview.grant_success",
            failure_event="tradingview.grant_failed",
            transport_event="tradingview.grant_transport_error",
        )

    async def update_access(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._post_with_retry(
            endpoint=self._update_endpoint,
            payload=payload,
            success_event="tradingview.update_success",
            failure_event="tradingview.update_failed",
            transport_event="tradingview.update_transport_error",
        )

    async def _post_with_retry(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        success_event: str,
        failure_event: str,
        transport_event: str,
    ) -> Dict[str, Any]:
        url = _join_url(self._base_url, endpoint)
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt <= self._max_retries:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                try:
                    response = await client.post(url, headers=self._headers, json=payload)
                    response.raise_for_status()
                    logger.info(
                        success_event,
                        extra={
                            "extra_data": {
                                "status_code": response.status_code,
                                "url": url,
                                "payload": {
                                    "scriptId": payload.get("scriptId"),
                                    "username": payload.get("username"),
                                },
                            }
                        },
                    )
                    return response.json()
                except httpx.HTTPStatusError as exc:
                    last_error = exc
                    logger.warning(
                        failure_event,
                        extra={
                            "extra_data": {
                                "attempt": attempt + 1,
                                "status_code": exc.response.status_code,
                                "payload": {
                                    "scriptId": payload.get("scriptId"),
                                    "username": payload.get("username"),
                                },
                            }
                        },
                    )
                except httpx.HTTPError as exc:
                    last_error = exc
                    logger.warning(
                        transport_event,
                        extra={
                            "extra_data": {
                                "attempt": attempt + 1,
                                "payload": {
                                    "scriptId": payload.get("scriptId"),
                                    "username": payload.get("username"),
                                },
                                "error": str(exc),
                            }
                        },
                    )
            if attempt == self._max_retries:
                break
            await asyncio.sleep(self._backoff[min(attempt, len(self._backoff) - 1)])
            attempt += 1

        raise ApiError(
            "TradingView request failed",
            status_code=getattr(last_error, "response", None).status_code
            if hasattr(last_error, "response")
            else None,
            payload=payload,
        )


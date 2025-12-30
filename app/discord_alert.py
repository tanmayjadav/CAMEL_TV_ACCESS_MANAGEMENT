"""
Discord Alert System

Sends alerts to Discord channel via webhook.
No authentication required as Discord webhooks only require the webhook URL.

Features:
- Send text alerts
- Send embed alerts
- Send files
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

try:
    from dhooks import Webhook, Embed, File
except ImportError:
    raise ImportError(
        "dhooks library is required. Install it with: pip install dhooks"
    )

logger = logging.getLogger(__name__)


class DiscordAlert:
    """Discord webhook alert system for sending notifications."""

    _colors = {
        "green": 0x2ECC71,
        "red": 0xE74C3C,
        "gold": 0xF1C40F,
        "yellow": 0xFFFF00,
        "blue": 0x3498DB,
        "pink": 0xFD0061,
        "purple": 0x9B59B6,
        "orange": 0xE67E22,
        "teal": 0x1ABC9C,
        "dark_red": 0x992D22,
        "dark_blue": 0x206694,
    }

    _default_color = "pink"

    def __init__(self, default_webhook_url: Optional[str] = None, default_author: Optional[str] = None):
        """
        Initialize DiscordAlert instance.

        Args:
            default_webhook_url: Optional default webhook URL for all alerts
            default_author: Optional default author/username for all alerts
        """
        self._default_webhook_url = default_webhook_url
        self._default_author = default_author
        self._whitelist = ["all"]

    def set_whitelist(self, whitelist: List[str]) -> None:
        """
        Set whitelist for embed field filtering.

        Args:
            whitelist: List of field names to include in embeds. Use ["all"] to include all fields.
        """
        if not isinstance(whitelist, list):
            raise ValueError("Whitelist must be a list")
        self._whitelist = whitelist

    def _send_text_alert(
        self,
        webhook_url: str,
        message: str,
        author: Optional[str] = None,
    ) -> None:
        """
        Send text alert to Discord webhook.

        Args:
            webhook_url: Discord webhook URL
            message: Text message to send
            author: Optional author/username override
        """
        try:
            hook = Webhook(webhook_url)
            kwargs: Dict[str, Any] = {}
            if author:
                kwargs["username"] = author
            elif self._default_author:
                kwargs["username"] = self._default_author

            threading.Thread(
                target=hook.send,
                args=(str(message),),
                kwargs=kwargs,
                daemon=True,
            ).start()
        except Exception as e:
            logger.error(
                "discord.text_alert_failed",
                extra={
                    "extra_data": {
                        "error": str(e),
                        "webhook_url": webhook_url[:50] + "..." if len(webhook_url) > 50 else webhook_url,
                    }
                },
            )

    def _send_embed_alert(
        self,
        webhook_url: str,
        message: Dict[str, Any],
        author: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        Send embed alert to Discord webhook.

        Args:
            webhook_url: Discord webhook URL
            message: Dictionary containing embed fields and metadata
            author: Optional author/username override
            title: Optional embed title
            description: Optional embed description
        """
        try:
            # Extract color from message or use default
            color_name = message.get("color", self._default_color)
            color = self._colors.get(color_name, self._colors[self._default_color])

            # Build embed options
            embed_options: Dict[str, Any] = {
                "color": color,
                "timestamp": "now",
            }

            if title:
                embed_options["title"] = title
            if description:
                embed_options["description"] = description

            embed = Embed(**embed_options)

            # Add fields based on whitelist
            for key, value in message.items():
                # Skip special fields that are handled separately
                if key in ("color", "title", "description"):
                    continue

                # Check if field should be included
                if "all" in self._whitelist or key in self._whitelist:
                    embed.add_field(
                        name=str(key).upper().replace("_", " "),
                        value=str(value),
                        inline=False,
                    )

            # Set up webhook
            hook = Webhook(webhook_url)
            kwargs: Dict[str, Any] = {"embed": embed}

            # Set author if provided
            author_name = author or self._default_author
            if author_name:
                embed.set_author(name=author_name)
                kwargs["username"] = author_name

            threading.Thread(
                target=hook.send,
                kwargs=kwargs,
                daemon=True,
            ).start()
        except Exception as e:
            logger.error(
                "discord.embed_alert_failed",
                extra={
                    "extra_data": {
                        "error": str(e),
                        "webhook_url": webhook_url[:50] + "..." if len(webhook_url) > 50 else webhook_url,
                    }
                },
            )

    def send_alert(
        self,
        webhook_url: Optional[str] = None,
        author: Optional[str] = None,
        message: Union[str, Dict[str, Any]] = ...,
        use_embed: bool = True,
    ) -> None:
        """
        Send alert to Discord webhook (text or embed).

        Args:
            webhook_url: Discord webhook URL (uses default if not provided)
            author: Optional author/username override
            message: Text string or dictionary for embed
            use_embed: Whether to send as embed (True) or text (False)

        Raises:
            ValueError: If webhook_url is not provided and no default is set
            ValueError: If message is not provided
        """
        if message is ...:
            raise ValueError("message parameter is required")

        webhook = webhook_url or self._default_webhook_url
        if not webhook:
            raise ValueError(
                "webhook_url must be provided or set as default during initialization"
            )

        if use_embed:
            if isinstance(message, str):
                # Convert string to embed format
                message_dict = {"description": message}
            elif isinstance(message, dict):
                message_dict = message.copy()
            else:
                raise ValueError("message must be a string or dictionary when use_embed=True")

            # Extract title and description if present
            title = message_dict.pop("title", None)
            description = message_dict.pop("description", None)

            self._send_embed_alert(
                webhook_url=webhook,
                message=message_dict,
                author=author,
                title=title,
                description=description,
            )
        else:
            if not isinstance(message, str):
                raise ValueError("message must be a string when use_embed=False")
            self._send_text_alert(
                webhook_url=webhook,
                message=message,
                author=author,
            )

    def send_file(
        self,
        webhook_url: Optional[str] = None,
        file_to_send: Union[str, Path, bytes, Any] = ...,
        file_name: Optional[str] = None,
        author: Optional[str] = None,
    ) -> None:
        """
        Send file to Discord webhook.

        Args:
            webhook_url: Discord webhook URL (uses default if not provided)
            file_to_send: File path (str/Path), file-like object, or bytes
            file_name: Optional file name (required if file_to_send is bytes)
            author: Optional author/username override

        Raises:
            ValueError: If webhook_url is not provided and no default is set
            ValueError: If file_to_send is not provided
            ValueError: If file_name is required but not provided
        """
        if file_to_send is ...:
            raise ValueError("file_to_send parameter is required")

        webhook = webhook_url or self._default_webhook_url
        if not webhook:
            raise ValueError(
                "webhook_url must be provided or set as default during initialization"
            )

        try:
            hook = Webhook(webhook)

            # Handle different file input types
            if isinstance(file_to_send, (str, Path)):
                file_path = Path(file_to_send)
                if not file_path.exists():
                    raise FileNotFoundError(f"File not found: {file_path}")
                if not file_name:
                    file_name = file_path.name
                _file = File(str(file_path), name=file_name)
            elif isinstance(file_to_send, bytes):
                if not file_name:
                    raise ValueError("file_name is required when file_to_send is bytes")
                _file = File(file_to_send, name=file_name)
            else:
                # Assume it's a file-like object
                if not file_name:
                    raise ValueError("file_name is required when file_to_send is a file-like object")
                _file = File(file_to_send, name=file_name)

            kwargs: Dict[str, Any] = {"file": _file}

            author_name = author or self._default_author
            if author_name:
                kwargs["username"] = author_name

            threading.Thread(
                target=hook.send,
                kwargs=kwargs,
                daemon=True,
            ).start()
        except Exception as e:
            logger.error(
                "discord.file_send_failed",
                extra={
                    "extra_data": {
                        "error": str(e),
                        "file_name": file_name,
                        "webhook_url": webhook[:50] + "..." if len(webhook) > 50 else webhook,
                    }
                },
            )
            raise


# Utility functions for easy integration with settings
def get_discord_alert(settings: Any) -> Optional[DiscordAlert]:
    """
    Get a DiscordAlert instance from settings if Discord is configured and enabled.

    Args:
        settings: Application settings (from app.config.Settings)

    Returns:
        DiscordAlert instance if Discord is configured and enabled, None otherwise
    """
    if not settings.discord:
        return None

    if not settings.discord.enabled:
        return None

    if not settings.discord.webhook_url:
        return None

    return DiscordAlert(
        default_webhook_url=settings.discord.webhook_url,
        default_author=settings.discord.author,
    )


def send_discord_alert_if_enabled(
    settings: Any,
    message: Union[str, Dict[str, Any]],
    use_embed: bool = True,
    **kwargs,
) -> bool:
    """
    Send a Discord alert if Discord is enabled in settings.

    Args:
        settings: Application settings (from app.config.Settings)
        message: Message to send (string or dict)
        use_embed: Whether to send as embed (default: True)
        **kwargs: Additional arguments to pass to send_alert

    Returns:
        True if alert was sent, False otherwise
    """
    alert = get_discord_alert(settings)
    if not alert:
        return False

    try:
        alert.send_alert(message=message, use_embed=use_embed, **kwargs)
        return True
    except Exception:
        # Silently fail - we don't want Discord failures to break the main flow
        return False


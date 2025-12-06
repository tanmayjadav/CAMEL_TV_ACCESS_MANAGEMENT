from __future__ import annotations

import asyncio
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Iterable, List, Optional

logger = logging.getLogger(__name__)


def _send_email_sync(
    to_email: str,
    subject: str,
    html_body: str,
    from_email: str,
    smtp_server: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    bcc: Optional[Iterable[str]] = None,
) -> None:
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = from_email
    message["To"] = to_email

    part = MIMEText(html_body, "html")
    message.attach(part)

    recipients: List[str] = [to_email]
    if bcc:
        recipients.extend([addr for addr in bcc if addr])

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
        server.login(smtp_user, smtp_password)
        server.sendmail(from_email, recipients, message.as_string())

    logger.info(
        "email.sent",
        extra={
            "extra_data": {
                "to": to_email,
                "bcc": recipients[1:] if len(recipients) > 1 else [],
            }
        },
    )


async def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    from_email: str,
    smtp_server: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    bcc: Optional[Iterable[str]] = None,
) -> None:
    try:
        await asyncio.to_thread(
            _send_email_sync,
            to_email,
            subject,
            html_body,
            from_email,
            smtp_server,
            smtp_port,
            smtp_user,
            smtp_password,
            bcc,
        )
    except Exception as exc:  # pragma: no cover - network dependent
        logger.error(
            "email.failed",
            extra={
                "extra_data": {
                    "to": to_email,
                    "error": str(exc),
                }
            },
        )
        raise

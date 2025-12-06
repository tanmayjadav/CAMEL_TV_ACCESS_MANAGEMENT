from __future__ import annotations

import asyncio
import pathlib
from typing import List, Dict

from app import load_settings
from app.email import send_email


TEMPLATE_PATH = pathlib.Path("app/templates/invalid_username.html")


def render_template(username: str, suggestions: List[Dict[str, str]]) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    if suggestions:
        items = "".join(
            f'<li style="color: #160c66; font-size: 15px;"><strong>{s.get("username")}</strong></li>'
            for s in suggestions
            if s.get("username")
        )
        suggestion_block = (
            '<h3 style="color: #ff8f0f; font-size: 20px;">Did you mean?</h3>'
            f'<ul style="color: #160c66; font-size: 15px; padding-left: 20px;">{items}</ul>'
        )
    else:
        suggestion_block = (
            '<p style="color: #160c66; font-size: 15px;">TradingView did not return any '
            "suggestions for the username you entered.</p>"
        )
    return template.format(username=username, suggestions=suggestion_block)


async def run() -> None:
    settings = load_settings()
    if not settings.email:
        raise SystemExit("Email configuration missing in config.json")

    to_email = "brandedgroup21@gmail.com"
    html_body = render_template(
        username="demo_username",
        suggestions=[{"username": "demo_user_1"}, {"username": "demo_user_2"}],
    )

    await send_email(
        to_email=to_email,
        subject="Test: TradingView username verification",
        html_body=html_body,
        from_email=settings.email.from_email,
        smtp_server=settings.email.smtp_server,
        smtp_port=settings.email.smtp_port,
        smtp_user=settings.email.smtp_user,
        smtp_password=settings.email.smtp_password,
        bcc=settings.email.bcc or [],
    )

    print(f"Test email sent to {to_email}")


if __name__ == "__main__":
    asyncio.run(run())

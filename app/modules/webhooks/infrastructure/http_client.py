from typing import Any

import httpx

from app.modules.webhooks.application.webhook_service import WebhookDeliveryError


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(str(exc)) from exc

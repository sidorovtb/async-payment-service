from typing import Any

from faststream.rabbit import Channel
from faststream.rabbit import RabbitBroker
from faststream.rabbit import RabbitQueue

from app.common.config import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )


class MessagePublisher:
    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self.broker = broker
        self.timeout = timeout

    async def publish(self, body: dict[str, Any], queue: RabbitQueue, correlation_id: str | None = None) -> None:
        await self.broker.publish(
            body,
            queue=queue,
            correlation_id=correlation_id,
            persist=True,
            mandatory=True,
            timeout=self.timeout,
        )

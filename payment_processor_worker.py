import asyncio
import logging
from typing import Any
from uuid import UUID

import httpx
from faststream import AckPolicy
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit import RabbitMessage
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.common.config import Settings
from app.common.config import settings
from app.db.session import init_database_connection_pool
from app.messaging.broker import MessagePublisher
from app.messaging.broker import create_broker
from app.messaging.outbox.repository import SQLAlchemyOutboxRepository
from app.messaging.queues import dead_queue
from app.messaging.queues import declare_queues
from app.messaging.queues import payments_queue
from app.messaging.queues import retry_queues
from app.modules.payments.application.payment_service import PaymentService
from app.modules.payments.domain.entities import PaymentStatus
from app.modules.payments.domain.protocols import PaymentGateway
from app.modules.payments.infrastructure.gateway import RandomPaymentGateway
from app.modules.payments.infrastructure.repository import SQLAlchemyPaymentRepository
from app.modules.webhooks.application.webhook_service import WebhookSender
from app.modules.webhooks.application.webhook_service import WebhookService
from app.modules.webhooks.infrastructure.http_client import HttpWebhookSender


class PaymentMessageHandler:
    def __init__(
        self,
        service: PaymentService,
        gateway: PaymentGateway,
        webhooks: WebhookService,
        publisher: MessagePublisher,
    ) -> None:
        self.service = service
        self.gateway = gateway
        self.webhooks = webhooks
        self.publisher = publisher

    async def handle(self, body: dict[str, Any]) -> None:
        attempt = int(body.get("attempt", 1))
        target_queue = None
        try:
            status = await self.service.process(
                UUID(body["payment_id"]),
                self.gateway,
                self.webhooks,
                final_attempt=attempt >= settings.payment_processing_max_attempts,
            )
        except Exception:
            logging.exception(f"Payment processing failed: {body = }")

            if attempt < settings.payment_processing_max_attempts:
                target_queue = retry_queues[attempt - 1]
            else:
                target_queue = dead_queue
        else:
            if status == PaymentStatus.FAILED:
                target_queue = dead_queue
        if target_queue is not None:
            await self.publisher.publish({**body, "attempt": attempt + 1}, target_queue)


def init_faststream_app(
    settings: Settings,
    broker: RabbitBroker,
    sessions: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    sender: WebhookSender,
) -> FastStream:
    app = FastStream(broker)
    publisher = MessagePublisher(broker, settings.publish_timeout)

    @broker.subscriber(payments_queue, ack_policy=AckPolicy.MANUAL)
    async def consume(body: dict[str, Any], msg: RabbitMessage) -> None:
        try:
            async with sessions() as session:
                payments = SQLAlchemyPaymentRepository(session)
                webhooks_service = WebhookService(payments, session.begin, sender)
                payment_service = PaymentService(payments, SQLAlchemyOutboxRepository(session), session.begin)

                await PaymentMessageHandler(
                    payment_service,
                    gateway,
                    webhooks_service,
                    publisher,
                ).handle(
                    body,
                )
            await msg.ack()

        except Exception:
            logging.exception(f"Failed to handle message, requeue: {body = }")
            await broker.publish(body, queue=payments_queue)
            await msg.ack()

    @app.on_startup
    async def startup() -> None:
        await broker.connect()
        await declare_queues(broker)

    return app


async def main() -> None:
    engine, sessions = init_database_connection_pool(settings.database_url)
    broker = create_broker(settings)
    try:
        async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
            app = init_faststream_app(settings, broker, sessions, RandomPaymentGateway(), HttpWebhookSender(client))
            await app.run()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

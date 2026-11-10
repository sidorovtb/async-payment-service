import logging
from typing import Any
from typing import Protocol

from app.modules.payments.domain.entities import Payment
from app.modules.payments.domain.protocols import PaymentRepository
from app.modules.payments.domain.protocols import Transaction


class WebhookDeliveryError(Exception):
    pass


class WebhookSender(Protocol):
    async def send(
        self,
        url: str,
        payload: dict[str, Any],
    ) -> None: ...


class WebhookService:
    def __init__(
        self,
        payments: PaymentRepository,
        transaction: Transaction,
        sender: WebhookSender,
    ) -> None:
        self.payments = payments
        self.transaction = transaction
        self.sender = sender

    async def deliver(self, payment: Payment) -> None:
        if payment.webhook_sent:
            return

        payment.webhook_attempts += 1

        async with self.transaction():
            await self.payments.save_webhook(payment)

        try:
            await self.sender.send(
                payment.webhook_url,
                {
                    "payment_id": str(payment.id),
                    "status": payment.status.value,
                    "processed_at": str(payment.processed_at),
                },
            )
        except WebhookDeliveryError:
            logging.exception(f"Webhook attempt {payment.webhook_attempts = } failed for {payment.id = }")
            raise

        payment.webhook_sent = True
        async with self.transaction():
            await self.payments.save_webhook(payment)

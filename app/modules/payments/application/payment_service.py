import logging
from uuid import UUID

from app.messaging.contracts import PaymentOutbox
from app.modules.payments.domain.entities import FinalStatus
from app.modules.payments.domain.entities import NewPayment
from app.modules.payments.domain.entities import Payment
from app.modules.payments.domain.entities import PaymentStatus
from app.modules.payments.domain.exceptions import PaymentNotFound
from app.modules.payments.domain.exceptions import PaymentProcessingError
from app.modules.payments.domain.protocols import PaymentGateway
from app.modules.payments.domain.protocols import PaymentRepository
from app.modules.payments.domain.protocols import Transaction
from app.modules.webhooks.application.webhook_service import WebhookService


class PaymentService:
    def __init__(self, payments: PaymentRepository, outbox: PaymentOutbox, transaction: Transaction) -> None:
        self.payments = payments
        self.outbox = outbox
        self.transaction = transaction

    async def create(self, key: str, data: NewPayment) -> Payment:
        async with self.transaction():
            payment_id = await self.payments.insert_pending(key, data)
            if payment_id is not None:
                await self.outbox.add(payment_id)
            return await self.payments.get_by_key(key)

    async def get(self, payment_id: UUID) -> Payment:
        payment = await self.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound("Payment not found")
        return payment

    async def process(
        self,
        payment_id: UUID,
        gateway: PaymentGateway,
        webhooks: WebhookService,
        *,
        final_attempt: bool = False,
    ) -> PaymentStatus:
        async with self.transaction():
            payment = await self.payments.get(payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFound(f"Unknown payment: {payment_id}")
            if payment.status == PaymentStatus.PENDING:
                try:
                    status = await gateway.charge(payment)
                except PaymentProcessingError:
                    if not final_attempt:
                        raise
                    logging.exception("Payment processing attempts exhausted: %s", payment.id)
                    status = FinalStatus.FAILED
                payment.complete(status)
                payment.updated_at, payment.processed_at = await self.payments.set_status(payment.id, status)

        await webhooks.deliver(payment)
        return payment.status

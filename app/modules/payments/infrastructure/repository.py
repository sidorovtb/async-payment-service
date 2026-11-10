from datetime import datetime
from uuid import UUID
from uuid import uuid4

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.payments.domain.entities import FinalStatus
from app.modules.payments.domain.entities import NewPayment
from app.modules.payments.domain.entities import Payment
from app.modules.payments.domain.entities import PaymentStatus
from app.modules.payments.infrastructure.models import PaymentModel


def to_payment(row: PaymentModel) -> Payment:
    return Payment(
        id=row.id,
        idempotency_key=row.idempotency_key,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        payment_metadata=row.payment_metadata,
        webhook_url=row.webhook_url,
        status=PaymentStatus(row.status),
        created_at=row.created_at,
        updated_at=row.updated_at,
        processed_at=row.processed_at,
        webhook_attempts=row.webhook_attempts,
        webhook_sent=row.webhook_sent,
    )


class SQLAlchemyPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_pending(self, key: str, data: NewPayment) -> UUID | None:
        return await self.session.scalar(
            insert(PaymentModel)
            .values(
                id=uuid4(),
                idempotency_key=key,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                payment_metadata=data.payment_metadata,
                webhook_url=data.webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[PaymentModel.idempotency_key])
            .returning(PaymentModel.id),
        )

    async def get_by_key(self, key: str) -> Payment:
        row = (await self.session.scalars(select(PaymentModel).where(PaymentModel.idempotency_key == key))).one()
        return to_payment(row)

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        query = select(PaymentModel).where(PaymentModel.id == payment_id)
        if for_update:
            query = query.with_for_update()
        row = await self.session.scalar(query)
        return to_payment(row) if row is not None else None

    async def set_status(self, payment_id: UUID, status: FinalStatus) -> tuple[datetime, datetime]:
        result = await self.session.execute(
            update(PaymentModel)
            .where(PaymentModel.id == payment_id)
            .values(status=status.value, processed_at=func.clock_timestamp())
            .returning(PaymentModel.updated_at, PaymentModel.processed_at),
        )
        updated_at, processed_at = result.one()
        return updated_at, processed_at

    async def save_webhook(self, payment: Payment) -> None:
        await self.session.execute(
            update(PaymentModel)
            .where(PaymentModel.id == payment.id)
            .values(webhook_attempts=payment.webhook_attempts, webhook_sent=payment.webhook_sent),
        )

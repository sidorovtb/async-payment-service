from datetime import datetime
from datetime import timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.outbox.models import OutboxModel
from app.modules.payments.infrastructure.models import PaymentModel


class SQLAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, payment_id: UUID, event_type: str = "payment.created") -> None:
        self.session.add(OutboxModel(payment_id=payment_id, aggregate_id=payment_id, event_type=event_type))

    async def get_next_unpublished(self, limit: int = 1) -> list[OutboxModel]:
        events = await self.session.scalars(
            select(OutboxModel)
            .where(OutboxModel.published_at.is_(None))
            .order_by(OutboxModel.id)
            .with_for_update(skip_locked=True)
            .limit(limit),
        )
        return list(events)

    def mark_published(self, event: OutboxModel) -> None:
        event.published_at = datetime.now(timezone.utc)

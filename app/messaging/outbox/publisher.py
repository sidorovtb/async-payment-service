import asyncio

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.messaging.broker import MessagePublisher
from app.messaging.contracts import PaymentMessage
from app.messaging.outbox.repository import SQLAlchemyOutboxRepository
from app.messaging.queues import payments_queue


class OutboxPublisher:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        publisher: MessagePublisher,
        batch_size: int = 100,
    ) -> None:
        if batch_size < 1:
            raise ValueError("Outbox batch size must be positive")
        self.sessions = sessions
        self.publisher = publisher
        self.batch_size = batch_size

    async def publish_batch(self) -> int:
        async with self.sessions.begin() as session:
            outbox = SQLAlchemyOutboxRepository(session)
            events = await outbox.get_next_unpublished(self.batch_size)
            if not events:
                return 0
            results = await asyncio.gather(
                *(
                    self.publisher.publish(
                        PaymentMessage(payment_id=str(event.aggregate_id), attempt=1),
                        payments_queue,
                        str(event.aggregate_id),
                    )
                    for event in events
                ),
                return_exceptions=True,
            )
            errors = []
            for event, result in zip(events, results, strict=True):
                if isinstance(result, BaseException):
                    errors.append(result)
                else:
                    outbox.mark_published(event)
        if errors:
            raise errors[0]
        return len(events)

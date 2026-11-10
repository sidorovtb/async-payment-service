from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.outbox.repository import SQLAlchemyOutboxRepository
from app.modules.payments.application.payment_service import PaymentService
from app.modules.payments.infrastructure.repository import SQLAlchemyPaymentRepository


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessions() as session:
        yield session


def get_payment_service(session: Annotated[AsyncSession, Depends(get_session)]) -> PaymentService:
    return PaymentService(SQLAlchemyPaymentRepository(session), SQLAlchemyOutboxRepository(session), session.begin)

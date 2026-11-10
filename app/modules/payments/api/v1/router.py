from typing import Annotated
from uuid import UUID

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Header
from pydantic import UUID4

from app.modules.payments.api.dependencies import get_payment_service
from app.modules.payments.api.v1.schemas import PaymentCreated
from app.modules.payments.api.v1.schemas import PaymentDetails
from app.modules.payments.api.v1.schemas import PaymentInput
from app.modules.payments.application.payment_service import PaymentService
from app.modules.payments.domain.entities import NewPayment
from app.modules.payments.domain.entities import Payment

router = APIRouter(prefix="/payments")


@router.post("", status_code=202, response_model=PaymentCreated)
async def create_payment(
    body: PaymentInput,
    idempotency_key: Annotated[UUID4, Header()],
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> Payment:
    return await service.create(
        str(idempotency_key),
        NewPayment(
            body.amount,
            body.currency,
            body.description,
            body.metadata,
            str(body.webhook_url),
        ),
    )


@router.get("/{payment_id}", response_model=PaymentDetails)
async def get_payment(
    payment_id: UUID,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> Payment:
    return await service.get(payment_id)

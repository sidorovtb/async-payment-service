from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import HttpUrl

from app.modules.payments.domain.entities import Currency
from app.modules.payments.domain.entities import PaymentStatus


class PaymentInput(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: HttpUrl


class PaymentCreated(BaseModel):
    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PaymentDetails(PaymentCreated):
    amount: Decimal
    currency: Currency
    updated_at: datetime | None
    processed_at: datetime | None
    description: str
    metadata: dict[str, Any] = Field(validation_alias="payment_metadata")
    webhook_url: str

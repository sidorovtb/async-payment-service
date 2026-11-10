from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from typing import Literal
from uuid import UUID


class PaymentStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class FinalStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


Currency = Literal["RUB", "USD", "EUR"]


@dataclass(frozen=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str
    payment_metadata: dict[str, Any]
    webhook_url: str


@dataclass
class Payment:
    id: UUID
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    payment_metadata: dict[str, Any]
    webhook_url: str
    status: PaymentStatus
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None
    webhook_attempts: int
    webhook_sent: bool

    def complete(self, status: FinalStatus) -> None:
        if self.status == PaymentStatus.PENDING:
            self.status = PaymentStatus(status.value)

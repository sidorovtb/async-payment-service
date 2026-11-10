from typing import Protocol
from typing import TypedDict
from uuid import UUID


class PaymentMessage(TypedDict):
    payment_id: str
    attempt: int


class PaymentOutbox(Protocol):
    async def add(self, payment_id: UUID, event_type: str = "payment.created") -> None: ...

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Any
from typing import Protocol
from uuid import UUID

from app.modules.payments.domain.entities import FinalStatus
from app.modules.payments.domain.entities import NewPayment
from app.modules.payments.domain.entities import Payment

Transaction = Callable[[], AbstractAsyncContextManager[Any]]


class PaymentRepository(Protocol):
    async def insert_pending(self, key: str, data: NewPayment) -> UUID | None: ...

    async def get_by_key(self, key: str) -> Payment: ...

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None: ...

    async def set_status(self, payment_id: UUID, status: FinalStatus) -> tuple[datetime, datetime]: ...

    async def save_webhook(self, payment: Payment) -> None: ...


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> FinalStatus: ...

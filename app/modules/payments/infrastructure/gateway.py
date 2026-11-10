import asyncio
import random

from app.modules.payments.domain.entities import FinalStatus
from app.modules.payments.domain.entities import Payment
from app.modules.payments.domain.exceptions import PaymentProcessingError


class RandomPaymentGateway:
    async def charge(self, payment: Payment) -> FinalStatus:
        await asyncio.sleep(random.uniform(2, 5))
        if random.random() < 0.9:
            return FinalStatus.SUCCEEDED
        raise PaymentProcessingError(f"Payment processing failed: {payment.id}")

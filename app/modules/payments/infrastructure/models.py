from datetime import datetime
from decimal import Decimal
from uuid import UUID
from uuid import uuid4

from sqlalchemy import DateTime
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from app.db.base import Base
from app.modules.payments.domain.entities import PaymentStatus


class PaymentModel(Base):
    __tablename__ = "payments"
    __table_args__ = ()

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str]
    payment_metadata: Mapped[dict] = mapped_column("metadata", JSONB)
    webhook_url: Mapped[str]
    status: Mapped[str] = mapped_column(String(16), default=PaymentStatus.PENDING.value)
    webhook_attempts: Mapped[int] = mapped_column(default=0)
    webhook_sent: Mapped[bool] = mapped_column(default=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now())

from collections.abc import AsyncIterator
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID
from uuid import uuid4

import httpx
import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.schema import DropSchema

from api import app
from app.common.config import settings
from app.db.session import init_database_connection_pool
from app.messaging.broker import MessagePublisher
from app.messaging.outbox.models import OutboxModel
from app.messaging.outbox.repository import SQLAlchemyOutboxRepository
from app.modules.payments.application.payment_service import PaymentService
from app.modules.payments.infrastructure.models import PaymentModel
from app.modules.payments.infrastructure.repository import SQLAlchemyPaymentRepository
from app.modules.webhooks.application.webhook_service import WebhookService


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def sessions() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    schema = "test_payments_" + uuid4().hex
    engine, _ = init_database_connection_pool(settings.database_url)
    engine = engine.execution_options(schema_translate_map={None: schema})
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))

    def migrate(connection):
        with Operations.context(MigrationContext.configure(connection)):
            for revision in reversed(list(scripts.walk_revisions())):
                revision.module.upgrade()

    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
            await connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            await connection.run_sync(migrate)
        try:
            yield async_sessionmaker(engine, expire_on_commit=False)
        finally:
            async with engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
    finally:
        await engine.dispose()


@pytest.fixture
async def client(sessions) -> AsyncIterator[httpx.AsyncClient]:
    previous = getattr(app.state, "sessions", None)
    app.state.sessions = sessions
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as client:
            yield client
    finally:
        if previous is None:
            del app.state.sessions
        else:
            app.state.sessions = previous


@pytest.fixture
def headers() -> dict[str, str]:
    return {"X-API-Key": settings.api_key}


@pytest.fixture
def payment_body() -> dict:
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Regression",
        "metadata": {"order": 1},
        "webhook_url": "http://webhook/payment",
    }


@pytest.fixture
def idempotency_key() -> str:
    return str(uuid4())


@pytest.fixture
def create_payment(client, headers, payment_body, idempotency_key) -> Callable:
    async def create() -> UUID:
        response = await client.post(
            "/api/v1/payments",
            headers={**headers, "Idempotency-Key": idempotency_key},
            json=payment_body,
        )
        assert response.status_code == 202, response.text
        assert set(response.json()) == {"payment_id", "status", "created_at"}
        return UUID(response.json()["payment_id"])

    return create


@pytest.fixture
def read_payment(sessions) -> Callable:
    async def read(payment_id):
        async with sessions() as session:
            return await SQLAlchemyPaymentRepository(session).get(payment_id)

    return read


@pytest.fixture
def process_payment(sessions) -> Callable:
    async def process(payment_id, gateway, sender, *, final_attempt=False):
        async with sessions() as session:
            payments = SQLAlchemyPaymentRepository(session)
            webhooks = WebhookService(payments, session.begin, sender)
            service = PaymentService(payments, SQLAlchemyOutboxRepository(session), session.begin)
            return await service.process(payment_id, gateway, webhooks, final_attempt=final_attempt)

    return process


@pytest.fixture
def transport() -> AsyncMock:
    return AsyncMock(spec=MessagePublisher)


@pytest.fixture
def add_events(sessions) -> Callable:
    async def add(count) -> list[str]:
        ids = []
        async with sessions.begin() as session:
            outbox = SQLAlchemyOutboxRepository(session)
            for _ in range(count):
                payment_id = uuid4()
                session.add(
                    PaymentModel(
                        id=payment_id,
                        idempotency_key=str(uuid4()),
                        amount=Decimal("1"),
                        currency="RUB",
                        description="Batch test",
                        payment_metadata={},
                        webhook_url="http://webhook/",
                        updated_at=datetime.now(timezone.utc),
                    ),
                )
                await session.flush()
                await outbox.add(payment_id)
                ids.append(str(payment_id))
        return ids

    return add


@pytest.fixture
def published(sessions) -> Callable:
    async def read() -> dict[str, bool]:
        async with sessions() as session:
            events = (await session.scalars(select(OutboxModel).order_by(OutboxModel.id))).all()
            return {str(event.aggregate_id): event.published_at is not None for event in events}

    return read

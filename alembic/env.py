import asyncio

from alembic import context
from app.common.config import settings
from app.db.base import Base
from app.db.session import init_database_connection_pool
from app.messaging.outbox.models import OutboxModel
from app.modules.payments.infrastructure.models import PaymentModel


def run_migrations(connection):
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def main():
    engine, _ = init_database_connection_pool(settings.database_url)
    async with engine.connect() as connection:
        await connection.run_sync(run_migrations)
    await engine.dispose()


asyncio.run(main())

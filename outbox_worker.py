import asyncio
import logging

from app.common.config import settings
from app.db.session import init_database_connection_pool
from app.messaging.broker import MessagePublisher
from app.messaging.broker import create_broker
from app.messaging.outbox.publisher import OutboxPublisher
from app.messaging.queues import declare_queues


async def main() -> None:
    engine, sessions = init_database_connection_pool(settings.database_url)
    broker = create_broker(settings)
    publisher = OutboxPublisher(
        sessions,
        MessagePublisher(broker, settings.publish_timeout),
        settings.outbox_batch_size,
    )
    try:
        async with broker:
            await declare_queues(broker)
            while True:
                try:
                    if not await publisher.publish_batch():
                        await asyncio.sleep(settings.outbox_interval)
                except Exception:
                    logging.exception("Outbox publication failed")
                    await asyncio.sleep(settings.outbox_interval)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

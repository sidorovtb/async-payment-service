from faststream.rabbit import RabbitBroker
from faststream.rabbit import RabbitQueue

from app.common.config import settings

payments_queue = RabbitQueue("payments.new", durable=True)
dead_queue = RabbitQueue("payments.dlq", durable=True)


retry_queues = (
    RabbitQueue(
        "payments.retry.1",
        durable=True,
        arguments={
            "x-message-ttl": settings.retry_delay_s * 1000,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": payments_queue.name,
        },
    ),
    RabbitQueue(
        "payments.retry.2",
        durable=True,
        arguments={
            "x-message-ttl": settings.retry_delay_s * 1000 * 2,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": payments_queue.name,
        },
    ),
)


async def declare_queues(broker: RabbitBroker) -> None:
    await broker.declare_queue(payments_queue)

    for queue in retry_queues:
        await broker.declare_queue(queue)

    await broker.declare_queue(dead_queue)

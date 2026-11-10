import asyncio
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy import text

from app.messaging.outbox.models import OutboxModel
from app.messaging.outbox.publisher import OutboxPublisher
from app.messaging.queues import payments_queue

pytestmark = pytest.mark.anyio


async def test_outbox_event_metadata(sessions, add_events):
    payment_id = UUID((await add_events(1))[0])
    async with sessions() as session:
        event = (await session.scalars(select(OutboxModel))).one()
        assert event.payment_id == payment_id
        assert event.aggregate_id == payment_id
        assert event.event_type == "payment.created"
        assert event.created_at.tzinfo is not None
        assert event.published_at is None


async def test_outbox_sequence_supports_bigint(sessions, add_events):
    engine = sessions.kw["bind"]
    schema = engine.get_execution_options()["schema_translate_map"][None]
    async with sessions.begin() as session:
        sequence = await session.scalar(
            text("SELECT pg_get_serial_sequence(:table, 'id')"),
            {"table": f'"{schema}".outbox'},
        )
        await session.execute(
            text("SELECT setval(CAST(:sequence AS regclass), :value)"),
            {"sequence": sequence, "value": 2**31 - 1},
        )
    await add_events(1)
    async with sessions() as session:
        assert await session.scalar(select(OutboxModel.id)) == 2**31


async def test_batch_limit_and_empty_outbox(sessions, transport, add_events, published):
    ids = await add_events(5)
    publisher = OutboxPublisher(sessions, transport, batch_size=2)
    assert await publisher.publish_batch() == 2
    assert sum((await published()).values()) == 2
    assert await publisher.publish_batch() == 2
    assert await publisher.publish_batch() == 1
    assert await publisher.publish_batch() == 0
    assert transport.publish.await_count == 5
    for payment_id, args in zip(ids, transport.publish.await_args_list, strict=True):
        assert args.args == ({"payment_id": payment_id, "attempt": 1}, payments_queue, payment_id)


async def test_publications_run_concurrently(sessions, transport, add_events, published):
    await add_events(2)
    started = []
    ready = asyncio.Event()

    async def publish(body, queue, correlation_id):
        started.append(correlation_id)
        if len(started) == 2:
            ready.set()
        await asyncio.wait_for(ready.wait(), timeout=2)

    transport.publish.side_effect = publish
    assert await OutboxPublisher(sessions, transport, 2).publish_batch() == 2
    assert sum((await published()).values()) == 2


async def test_partial_failure_keeps_only_failed_event_pending(sessions, transport, add_events, published):
    ids = await add_events(3)

    async def publish(body, queue, correlation_id):
        if correlation_id == ids[1]:
            raise RuntimeError("confirmation lost")

    transport.publish.side_effect = publish
    publisher = OutboxPublisher(sessions, transport, 3)
    with pytest.raises(RuntimeError, match="confirmation lost"):
        await publisher.publish_batch()
    assert await published() == {ids[0]: True, ids[1]: False, ids[2]: True}
    transport.publish.side_effect = None
    assert await publisher.publish_batch() == 1
    assert all((await published()).values())
    assert transport.publish.await_count == 4
    assert transport.publish.await_args.args[2] == ids[1]


async def test_failed_publication_is_not_marked(sessions, transport, add_events, published):
    ids = await add_events(1)
    transport.publish.side_effect = RuntimeError("broker unavailable")
    publisher = OutboxPublisher(sessions, transport)
    with pytest.raises(RuntimeError, match="broker unavailable"):
        await publisher.publish_batch()
    assert await published() == {ids[0]: False}
    transport.publish.side_effect = None
    assert await publisher.publish_batch() == 1
    assert await published() == {ids[0]: True}


async def test_concurrent_publishers_skip_locked_events(sessions, transport, add_events, published):
    ids = await add_events(4)
    started = []
    ready = asyncio.Event()

    async def publish(body, queue, correlation_id):
        started.append(correlation_id)
        if len(started) == 4:
            ready.set()
        await asyncio.wait_for(ready.wait(), timeout=2)

    transport.publish.side_effect = publish
    publishers = [OutboxPublisher(sessions, transport, 2) for _ in range(2)]
    assert await asyncio.gather(*(publisher.publish_batch() for publisher in publishers)) == [2, 2]
    assert sorted(started) == sorted(ids)
    assert all((await published()).values())


@pytest.mark.parametrize("size", [0, -1])
def test_invalid_batch_size(transport, size):
    with pytest.raises(ValueError, match="must be positive"):
        OutboxPublisher(None, transport, size)

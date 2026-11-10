import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID
from uuid import uuid4

import httpx
import pytest
from fastapi.exceptions import ResponseValidationError
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update

from app.common.config import settings
from app.messaging.broker import create_broker
from app.messaging.outbox.models import OutboxModel
from app.messaging.queues import dead_queue
from app.messaging.queues import payments_queue
from app.messaging.queues import retry_queues
from app.modules.payments.application.payment_service import PaymentService
from app.modules.payments.domain.entities import FinalStatus
from app.modules.payments.domain.entities import NewPayment
from app.modules.payments.domain.entities import PaymentStatus
from app.modules.payments.domain.exceptions import PaymentProcessingError
from app.modules.payments.infrastructure.gateway import RandomPaymentGateway
from app.modules.payments.infrastructure import gateway as gateway_module
from app.modules.payments.infrastructure.models import PaymentModel
from app.modules.payments.infrastructure.repository import SQLAlchemyPaymentRepository
from app.messaging.outbox.repository import SQLAlchemyOutboxRepository
from app.modules.webhooks.application.webhook_service import WebhookDeliveryError
from app.modules.webhooks.application.webhook_service import WebhookService
from app.modules.webhooks.infrastructure.http_client import HttpWebhookSender
from payment_processor_worker import PaymentMessageHandler
from payment_processor_worker import init_faststream_app

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("key", [None, "wrong"])
async def test_authentication(client, key):
    headers = {} if key is None else {"X-API-Key": key}
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
    assert response.status_code == 401


async def test_missing_payment_returns_404(client, headers):
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


async def test_missing_idempotency_key(client, headers, payment_body):
    response = await client.post("/api/v1/payments", headers=headers, json=payment_body)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "invalid",
    [
        {"amount": "0"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "file:///tmp/a"},
    ],
)
async def test_invalid_payment(client, headers, payment_body, idempotency_key, invalid):
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Idempotency-Key": idempotency_key},
        json={**payment_body, **invalid},
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "key",
    ["order-1", "", "00000000-0000-1000-8000-000000000000", "00000000-0000-3000-8000-000000000000"],
)
async def test_idempotency_key_requires_uuid4(client, headers, payment_body, key):
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Idempotency-Key": key},
        json=payment_body,
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "idempotency-key"]


async def test_idempotency_key_is_normalized(client, headers, payment_body, idempotency_key, create_payment):
    payment_id = await create_payment()
    response = await client.post(
        "/api/v1/payments",
        headers={**headers, "Idempotency-Key": idempotency_key.upper()},
        json=payment_body,
    )
    assert response.status_code == 202
    assert response.json()["payment_id"] == str(payment_id)


async def test_get_payment(client, headers, add_events, read_payment):
    payment_id = UUID((await add_events(1))[0])
    payment = await read_payment(payment_id)
    response = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "payment_id": str(payment_id),
        "status": PaymentStatus.PENDING,
        "created_at": payment.created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": payment.updated_at.isoformat().replace("+00:00", "Z"),
        "processed_at": None,
        "amount": "1.00",
        "currency": "RUB",
        "description": "Batch test",
        "metadata": {},
        "webhook_url": "http://webhook/",
    }


async def test_get_new_pending_payment(client, headers, create_payment):
    payment_id = await create_payment()
    response = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == PaymentStatus.PENDING
    assert response.json()["processed_at"] is None


async def test_concurrent_idempotency_and_outbox(sessions, create_payment, read_payment, payment_body):
    ids = await asyncio.gather(*(create_payment() for _ in range(12)))
    assert len(set(ids)) == 1
    payment_body["amount"] = "999.00"
    assert await create_payment() == ids[0]
    payment = await read_payment(ids[0])
    assert payment.amount == Decimal("123.45")
    assert payment.status is PaymentStatus.PENDING
    assert payment.processed_at is None
    async with sessions() as session:
        count = await session.scalar(
            select(func.count()).select_from(OutboxModel).where(OutboxModel.payment_id == ids[0]),
        )
        assert count == 1


async def test_updated_at_is_maintained_on_update(sessions, create_payment, read_payment):
    payment_id = await create_payment()
    before = await read_payment(payment_id)
    assert before.updated_at is None
    assert before.processed_at is None
    async with sessions.begin() as session:
        await session.execute(update(PaymentModel).where(PaymentModel.id == payment_id).values(description="Updated"))
    after = await read_payment(payment_id)
    assert after.updated_at > before.created_at
    assert after.created_at == before.created_at
    assert after.processed_at is None


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
async def test_supported_currencies(currency, payment_body, create_payment, read_payment):
    payment_body["currency"] = currency
    payment_id = await create_payment()
    assert (await read_payment(payment_id)).currency == currency


async def test_outbox_failure_rolls_back_payment(sessions, idempotency_key):
    async with sessions() as session:
        outbox = SimpleNamespace(add=AsyncMock(side_effect=RuntimeError("outbox unavailable")))
        service = PaymentService(SQLAlchemyPaymentRepository(session), outbox, session.begin)
        with pytest.raises(RuntimeError, match="outbox unavailable"):
            await service.create(idempotency_key, NewPayment(Decimal("1"), "RUB", "Atomic", {}, "http://webhook/"))
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentModel)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxModel)) == 0


@pytest.mark.parametrize("status", [FinalStatus.SUCCEEDED, FinalStatus.FAILED])
async def test_process_final_statuses_and_redelivery(
    status,
    create_payment,
    read_payment,
    process_payment,
    payment_body,
):
    payment_id = await create_payment()
    gateway = SimpleNamespace(charge=AsyncMock(return_value=status))
    sender = SimpleNamespace(send=AsyncMock())
    await process_payment(payment_id, gateway, sender)
    first = await read_payment(payment_id)
    await process_payment(payment_id, gateway, sender)
    payment = await read_payment(payment_id)
    assert payment.status is PaymentStatus(status.value)
    assert payment.webhook_sent
    assert payment.webhook_attempts == 1
    assert payment.processed_at >= payment.created_at
    assert payment.updated_at >= payment.created_at
    assert payment.processed_at == first.processed_at
    assert payment.updated_at == first.updated_at
    gateway.charge.assert_awaited_once()
    sender.send.assert_awaited_once_with(
        payment_body["webhook_url"],
        {"payment_id": str(payment_id), "status": status.value, "processed_at": str(payment.processed_at)},
    )


@pytest.mark.parametrize("succeeds", [True, False])
async def test_webhook_retry(succeeds, create_payment, read_payment, process_payment):
    payment_id = await create_payment()
    payloads = []
    gateway = SimpleNamespace(charge=AsyncMock(return_value=FinalStatus.SUCCEEDED))

    def respond(request):
        payloads.append(json.loads(request.content))
        return httpx.Response(200 if succeeds and len(payloads) == 3 else 500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        sender = HttpWebhookSender(client)
        for attempt in range(3):
            if succeeds and attempt == 2:
                await process_payment(payment_id, gateway, sender)
            else:
                with pytest.raises(WebhookDeliveryError):
                    await process_payment(payment_id, gateway, sender)
        if succeeds:
            await process_payment(payment_id, gateway, sender)
    payment = await read_payment(payment_id)
    assert (
        payloads
        == [
            {
                "payment_id": str(payment_id),
                "status": PaymentStatus.SUCCEEDED,
                "processed_at": str(payment.processed_at),
            },
        ]
        * 3
    )
    assert payment.webhook_sent == succeeds
    assert payment.webhook_attempts == 3
    gateway.charge.assert_awaited_once()


async def test_faststream_consumer_invokes_service(sessions, create_payment, read_payment):
    payment_id = await create_payment()
    gateway = SimpleNamespace(charge=AsyncMock(return_value=FinalStatus.SUCCEEDED))
    sender = SimpleNamespace(send=AsyncMock())
    broker = create_broker(settings)
    init_faststream_app(settings, broker, sessions, gateway, sender)
    async with RabbitTestBroker(broker):
        for _ in range(2):
            await broker.publish({"payment_id": str(payment_id), "attempt": 1}, queue=payments_queue)
    assert (await read_payment(payment_id)).status == PaymentStatus.SUCCEEDED
    gateway.charge.assert_awaited_once()
    sender.send.assert_awaited_once()


@pytest.mark.parametrize("attempt,queue", [(1, retry_queues[0]), (2, retry_queues[1]), (3, dead_queue)])
async def test_retry_and_dlq(transport, attempt, queue):
    service = SimpleNamespace(process=AsyncMock(side_effect=RuntimeError("processing failed")))
    handler = PaymentMessageHandler(service, AsyncMock(), AsyncMock(), transport)
    payment_id = str(uuid4())
    await handler.handle({"payment_id": payment_id, "attempt": attempt})
    transport.publish.assert_awaited_once_with({"payment_id": payment_id, "attempt": attempt + 1}, queue)


@pytest.mark.parametrize("attempt", [1, 3])
async def test_publication_failure_propagates_for_nack(transport, attempt):
    service = SimpleNamespace(process=AsyncMock(side_effect=RuntimeError("processing failed")))
    transport.publish.side_effect = RuntimeError("confirmation lost")
    handler = PaymentMessageHandler(service, AsyncMock(), AsyncMock(), transport)
    with pytest.raises(RuntimeError, match="confirmation lost"):
        await handler.handle({"payment_id": str(uuid4()), "attempt": attempt})


@pytest.mark.parametrize("chance", [0.89, 0.9])
async def test_random_gateway_raises_on_processing_failure(monkeypatch, chance):
    monkeypatch.setattr(gateway_module.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(gateway_module.random, "random", lambda: chance)
    gateway = RandomPaymentGateway()
    payment = SimpleNamespace(id=uuid4())
    if chance < 0.9:
        assert await gateway.charge(payment) is FinalStatus.SUCCEEDED
    else:
        with pytest.raises(PaymentProcessingError):
            await gateway.charge(payment)


async def test_payment_can_succeed_on_third_attempt(create_payment, read_payment, process_payment):
    payment_id = await create_payment()
    gateway = SimpleNamespace(charge=AsyncMock(side_effect=[
        PaymentProcessingError("first attempt"),
        PaymentProcessingError("second attempt"),
        FinalStatus.SUCCEEDED,
    ]))
    sender = SimpleNamespace(send=AsyncMock())
    for _ in range(2):
        with pytest.raises(PaymentProcessingError):
            await process_payment(payment_id, gateway, sender)
        payment = await read_payment(payment_id)
        assert payment.status is PaymentStatus.PENDING
        assert payment.processed_at is None
        sender.send.assert_not_awaited()
    assert await process_payment(payment_id, gateway, sender, final_attempt=True) is PaymentStatus.SUCCEEDED
    payment = await read_payment(payment_id)
    assert payment.processed_at is not None
    assert gateway.charge.await_count == 3
    sender.send.assert_awaited_once()


@pytest.mark.parametrize("webhook_fails", [False, True])
async def test_processing_exhaustion_marks_failed_and_sends_to_dlq(
    sessions, create_payment, read_payment, transport, webhook_fails,
):
    payment_id = await create_payment()
    gateway = SimpleNamespace(charge=AsyncMock(side_effect=PaymentProcessingError("gateway unavailable")))
    webhook_error = WebhookDeliveryError("webhook unavailable") if webhook_fails else None
    sender = SimpleNamespace(send=AsyncMock(side_effect=webhook_error))
    async with sessions() as session:
        payments = SQLAlchemyPaymentRepository(session)
        service = PaymentService(payments, SQLAlchemyOutboxRepository(session), session.begin)
        webhooks = WebhookService(payments, session.begin, sender)
        handler = PaymentMessageHandler(service, gateway, webhooks, transport)
        for attempt in (1, 2, 3):
            await handler.handle({"payment_id": str(payment_id), "attempt": attempt})
            payment = await read_payment(payment_id)
            assert payment.status is (PaymentStatus.PENDING if attempt < 3 else PaymentStatus.FAILED)
    assert payment.processed_at is not None
    assert payment.webhook_sent is not webhook_fails
    assert payment.webhook_attempts == 1
    assert gateway.charge.await_count == 3
    sender.send.assert_awaited_once_with(
        payment.webhook_url,
        {"payment_id": str(payment_id), "status": "failed", "processed_at": str(payment.processed_at)},
    )
    assert [args.args[1] for args in transport.publish.await_args_list] == [*retry_queues, dead_queue]


async def test_dlq_publication_is_retried_without_charging_again(sessions, create_payment, read_payment, transport):
    payment_id = await create_payment()
    gateway = SimpleNamespace(charge=AsyncMock(side_effect=PaymentProcessingError("gateway unavailable")))
    sender = SimpleNamespace(send=AsyncMock())
    async with sessions() as session:
        payments = SQLAlchemyPaymentRepository(session)
        service = PaymentService(payments, SQLAlchemyOutboxRepository(session), session.begin)
        handler = PaymentMessageHandler(service, gateway, WebhookService(payments, session.begin, sender), transport)
        for attempt in (1, 2):
            await handler.handle({"payment_id": str(payment_id), "attempt": attempt})
        transport.publish.side_effect = RuntimeError("DLQ confirmation lost")
        body = {"payment_id": str(payment_id), "attempt": 3}
        with pytest.raises(RuntimeError, match="DLQ confirmation lost"):
            await handler.handle(body)
        failed = await read_payment(payment_id)
        assert failed.status is PaymentStatus.FAILED
        transport.publish.side_effect = None
        await handler.handle(body)
    payment = await read_payment(payment_id)
    assert payment.processed_at == failed.processed_at
    assert gateway.charge.await_count == 3
    sender.send.assert_awaited_once()
    assert [args.args[1] for args in transport.publish.await_args_list] == [*retry_queues, dead_queue, dead_queue]


async def test_webhook_exhaustion_preserves_succeeded_status(sessions, create_payment, read_payment, transport):
    payment_id = await create_payment()
    gateway = SimpleNamespace(charge=AsyncMock(return_value=FinalStatus.SUCCEEDED))
    sender = SimpleNamespace(send=AsyncMock(side_effect=WebhookDeliveryError("webhook unavailable")))
    async with sessions() as session:
        payments = SQLAlchemyPaymentRepository(session)
        service = PaymentService(payments, SQLAlchemyOutboxRepository(session), session.begin)
        handler = PaymentMessageHandler(service, gateway, WebhookService(payments, session.begin, sender), transport)
        for attempt in (1, 2, 3):
            await handler.handle({"payment_id": str(payment_id), "attempt": attempt})
    assert (await read_payment(payment_id)).status is PaymentStatus.SUCCEEDED
    gateway.charge.assert_awaited_once()
    assert sender.send.await_count == 3
    assert transport.publish.await_args.args[1] == dead_queue

# Асинхронные платежи

Запуск: `docker compose up --build`. API: `http://localhost:8000/api/docs` и `http://localhost:8000/api/api-reference`, RabbitMQ: `http://localhost:15672`. Миграции применяются автоматически перед запуском API.

Для перехода на `https` рекомендуется поставить `Caddy`, `Nginx` или `Traefik` перед контейнером.

```sh
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: test-api' \
  -H 'Idempotency-Key: 8a3d4ef0-8f3c-4e9b-8a5d-29ec90716e82' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"100.00","currency":"RUB","description":"Заказ","metadata":{"order_id":1},"webhook_url":"http://my_host:8000/webhook"}'

curl http://localhost:8000/api/v1/payments/PAYMENT_ID \
  -H 'X-API-Key: test-api'
```
В Compose каждый процесс — отдельный сервис: `api`, `consumer`, `outbox_worker`; инфраструктура — `postgres`, `rabbitmq`. Старые команды `uvicorn api:app`, `python consumer.py`, `python publisher.py` доступны как совместимые точки входа к тому же коду после установки пакета или настройки `PYTHONPATH`.

Тесты используют pytest и асинхронный плагин уже установленного AnyIO. Общие фикстуры находятся в `tests/conftest.py`; каждый тест применяет миграции в отдельной временной схеме PostgreSQL и удаляет её после завершения. HTTP и транспорт consumer подменяются в тестах. Останавливать воркеры не требуется.

```sh
docker compose up -d postgres
docker compose build api
docker compose run --rm --no-deps api sh -c "pip install -r requirements.txt && python -m pytest"
```

Для локального запуска с настроенными переменными окружения: `pip install -r requirements.txt`, затем `python -m pytest`. Также можно задать переменных окружения используя `pip install python-dotenv` с последующим  `dotenv run -- <команда>`, например `dotenv run -- uvicorn api:app --host 0.0.0.0 --port 8000`. Также можно использовать скрипты запуска из `launch.json` и VS Code Tesing extension для тестов.


Точки запуска после `pip install -e .`:

```sh
alembic upgrade head
uvicorn api:app --host 0.0.0.0 --port 8000
python -m app.workers.payment_consumer
python -m app.workers.outbox_worker
```

Webhook должен быть доступен из consumer-контейнера. Он получает POST с `payment_id` и `status`. Суммы передаются десятичными строками. Повторный ключ возвращает существующий платёж с его текущим статусом, даже если тело запроса изменено.

Допустимые валюты: `RUB`, `USD`, `EUR`. `processed_at` заполняется при завершении обработки и не меняется при повторной доставке. У `pending` значение `processed_at` — `null`.

Outbox хранит `id` типа BIGINT, `aggregate_id` (UUID платежа), `event_type` (`payment.created`) и автоматически заполняемый PostgreSQL `created_at`. Поле `payment_id` сохранено для совместимости. Единственная начальная миграция `001_init.py` сразу создаёт актуальную схему payments и outbox.

Платёж `pending` и outbox создаются одной транзакцией. Отдельный процесс `outbox_worker` читает outbox пачками до `OUTBOX_BATCH_SIZE` событий и параллельно публикует отдельные сообщения с подтверждением каждого от RabbitMQ. При частичной ошибке подтверждённые события отмечаются опубликованными, остальные остаются для повтора. Блокировка `FOR UPDATE SKIP LOCKED` позволяет нескольким publisher-процессам брать разные пачки. Очереди durable, сообщения persistent. Возможна повторная доставка: consumer блокирует строку платежа и обрабатывает бизнес-операцию только для `pending`. Каждая попытка шлюза занимает 2–5 секунд: успех с вероятностью 90%, иначе возникает ошибка обработки.

При исключении сообщение повторно публикуется с номером попытки после задержек `RETRY_DELAY_S` и `2 * RETRY_DELAY_S` секунд: всего три попытки, затем подтверждённая публикация в `payments.dlq`. Исходное сообщение подтверждается после успешной обработки или публикации; сбой публикации вызывает возврат в очередь. После первых двух ошибок шлюза платёж остаётся `pending`. На третьей неудаче статус становится `failed`, фиксируется `processed_at`, выполняется попытка webhook и сообщение отправляется в DLQ. Ошибка webhook уже успешного платежа сохраняет статус `succeeded`. Повторная доставка завершённого платежа не вызывает шлюз повторно.

Webhook использует тот же лимит попыток с теми же экспоненциальными задержками. Их число сохраняется в БД до отправки; после исчерпания сообщение проходит оставшиеся попытки обработки и попадает в DLQ. Конечный статус при этом сохраняется. При сбое между HTTP-ответом и записью результата webhook может повториться, поэтому получателю нужна идемпотентность по `payment_id`. Завершённый платёж повторно не обрабатывается; незавершённая доставка webhook продолжается в пределах лимита.

Код находится в `app`: `modules/payments` содержит API, PaymentService с методами create/get/process, независимые доменные сущности и SQLAlchemy-адаптеры; `modules/webhooks` — WebhookService с методом deliver и HTTP-адаптер. `messaging` отвечает за RabbitMQ и outbox, `workers` — за запуск процессов. Зависимости передаются явно; API собирает сервис через Depends, воркеры — через функции. Транзакциями управляют сервисы, создание платежа использует одну сессию для обоих репозиториев. Вся схема создаётся одной начальной миграцией `001_init.py`.


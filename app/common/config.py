import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    rabbitmq_url: str
    api_key: str
    outbox_interval: float
    retry_delay_s: int
    webhook_timeout: float
    publish_timeout: float
    api_base_url: str
    outbox_batch_size: int
    api_version: str
    payment_processing_max_attempts: int
    payment_processing_max_parallelism: int


settings = Settings(
    database_url=os.environ["DATABASE_URL"],
    rabbitmq_url=os.environ["RABBITMQ_URL"],
    api_key=os.environ["API_KEY"],
    outbox_interval=float(os.getenv("OUTBOX_INTERVAL", "1")),
    outbox_batch_size=int(os.getenv("OUTBOX_BATCH_SIZE", "100")),
    retry_delay_s=int(os.getenv("RETRY_DELAY_S", "1")),
    webhook_timeout=float(os.getenv("WEBHOOK_TIMEOUT", "10")),
    publish_timeout=float(os.getenv("PUBLISH_TIMEOUT", "10")),
    api_base_url=os.getenv("API_BASE_URL", "/api"),
    api_version=os.getenv("API_VERSION", "unknown"),
    payment_processing_max_attempts=int(os.getenv("PAYMENT_PROCESSING_MAX_ATTEMPTS", "3")),
    payment_processing_max_parallelism=int(os.getenv("PAYMENT_PROCESSING_MAX_PARALLELISM", "5")),
)

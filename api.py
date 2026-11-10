from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import APIRouter
from fastapi import Depends
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.common.auth import validate_api_key
from app.common.config import settings
from app.db.session import init_database_connection_pool
from app.modules.payments.api.exception_handlers import payment_not_found_handler
from app.modules.payments.api.v1.router import router as payments_v1_router
from app.modules.payments.domain.exceptions import PaymentNotFound


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    engine, sessions = init_database_connection_pool(settings.database_url)
    app.state.sessions = sessions
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    lifespan=lifespan,
    description="Payment processor",
    title=f"Payment processor v {settings.api_version}",
    docs_url=f"{settings.api_base_url}/docs",
    openapi_url=f"{settings.api_base_url}/docs/openapi.json",
    redoc_url=f"{settings.api_base_url}/api-reference",
)


@app.get("/health")
async def api_health() -> str:
    return "OK"


instrumentator = Instrumentator(excluded_handlers=["/health", "/metrics"], should_group_status_codes=False)
instrumentator.instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

api_router = APIRouter(dependencies=[Depends(validate_api_key)], prefix=settings.api_base_url)

api_router.include_router(payments_v1_router, prefix="/v1", tags=["Payments"])

app.include_router(api_router)
app.add_exception_handler(PaymentNotFound, payment_not_found_handler)

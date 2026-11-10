from fastapi import Request
from fastapi.responses import JSONResponse

from app.modules.payments.domain.exceptions import PaymentNotFound


async def payment_not_found_handler(
    request: Request,
    exc: PaymentNotFound,
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={"detail": str(exc)},
    )

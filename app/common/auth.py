import secrets
from typing import Annotated

from fastapi import Header
from fastapi import HTTPException

from app.common.config import settings


async def validate_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    if x_api_key is None or not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(401, "Invalid API key")

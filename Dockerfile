FROM python:3.14.10-slim-trixie
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN python -m pip install -r requirements.txt
COPY . .

RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
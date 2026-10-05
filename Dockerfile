FROM python:3.11-slim

# Устанавливаем системные зависимости, если нужны (например, для сборки asyncpg/psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем Poetry для установки зависимостей из lock-файла
RUN pip install --no-cache-dir poetry==2.5.1
ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# Копируем pyproject.toml
COPY pyproject.toml poetry.lock /app/
COPY certs /app/certs
ENV POETRY_CERTIFICATES_TBANK_CERT=/app/certs/tbank-ca.pem

# Устанавливаем зависимости из poetry.lock
RUN poetry sync --only main --no-root --no-interaction

# Копируем исходный код
COPY ./app /app/app

# Открываем порт
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

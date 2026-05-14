FROM python:3.11-slim

# Устанавливаем системные зависимости, если нужны (например, для сборки asyncpg/psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем uv для быстрого разрешения зависимостей
RUN pip install --no-cache-dir uv

WORKDIR /app

# Копируем pyproject.toml
COPY pyproject.toml /app/

# Устанавливаем зависимости на системном уровне с помощью uv
RUN uv pip install --system .

# Копируем исходный код
COPY ./app /app/app

# Открываем порт
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

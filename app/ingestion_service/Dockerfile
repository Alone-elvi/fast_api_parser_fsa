# Dockerfile для ingestion_service
FROM fsa-base:latest

USER root

WORKDIR /app

# Установка системных зависимостей для pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения в правильную структуру
COPY . /app/ingestion_service/

ENV PYTHONPATH=/app
RUN chmod -R 755 /app

CMD ["dockerize", "-wait", "tcp://kafka:9092", "-wait", "tcp://postgres:5432", "--timeout", "300s", "uvicorn", "ingestion_service.main:app", "--host", "0.0.0.0", "--port", "8001", "--log-level", "info", "--reload"]

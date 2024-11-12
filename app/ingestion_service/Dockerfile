# Dockerfile для ingestion_service
FROM python:3.12-slim

# Установка dockerize и других утилит
RUN apt-get update && \
    apt-get install -y wget curl iputils-ping && \
    wget https://github.com/jwilder/dockerize/releases/download/v0.6.1/dockerize-linux-amd64-v0.6.1.tar.gz && \
    tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.6.1.tar.gz && \
    rm dockerize-linux-amd64-v0.6.1.tar.gz && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Установка дополнительных зависимостей
RUN pip install --no-cache-dir \
    pandas \
    openpyxl \
    python-multipart \
    fastapi \
    "uvicorn[standard]" \
    asyncpg \
    clickhouse-driver \
    kafka-python

# Копируем код приложения
COPY ./app /app

# Устанавливаем переменную окружения для Python path
ENV PYTHONPATH=/app
RUN chmod -R 755 /app
CMD ["dockerize", "-wait", "tcp://kafka:9092", "-wait", "tcp://postgres:5432", "--timeout", "300s", "uvicorn", "ingestion_service.main:app", "--host", "0.0.0.0", "--port", "8001", "--log-level", "info", "--reload"]

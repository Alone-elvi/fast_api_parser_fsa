# Dockerfile для ingestion_service
FROM python:3.12-slim

WORKDIR /app

# Добавление репозиториев, установка зависимостей и необходимых пакетов
RUN echo "deb http://deb.debian.org/debian/ bullseye main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian/ bullseye-updates main" >> /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bullseye-security main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y netcat-openbsd gcc libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Копирование кода и скрипта ожидания
COPY ./app/common /app/common
COPY ./app/ingestion_service/main.py /app
COPY ./requirements.txt /app
COPY ./app/ingestion_service/wait-for-it.sh /app

# Сделать скрипт исполняемым
RUN chmod +x /app/wait-for-it.sh

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Ожидание готовности сервисов и запуск
CMD ["/bin/sh", "-c", "./wait-for-it.sh kafka:9092 --timeout=60 --strict -- ./wait-for-it.sh postgres:5432 --timeout=60 --strict -- uvicorn main:app --host 0.0.0.0 --port 8001"]
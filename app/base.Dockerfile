FROM python:3.12-slim

USER root

# Установка только dockerize
RUN apt-get update && apt-get install -y wget \
    && wget https://github.com/jwilder/dockerize/releases/download/v0.6.1/dockerize-linux-amd64-v0.6.1.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.6.1.tar.gz \
    && rm dockerize-linux-amd64-v0.6.1.tar.gz \
    && apt-get remove -y wget \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Создаем пользователя и группу с фиксированными ID
RUN groupadd -g 1000 appgroup && \
    useradd -m -u 1000 -g appgroup appuser

WORKDIR /app
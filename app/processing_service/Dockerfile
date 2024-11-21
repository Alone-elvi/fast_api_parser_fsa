FROM fsa-base:latest

USER root

WORKDIR /app

# Установка dockerize и необходимых пакетов
ENV DOCKERIZE_VERSION v0.6.1
RUN apt-get update && apt-get install -y wget curl \
    && wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Копируем и устанавливаем зависимости
COPY ./app/processing_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для мониторинга
RUN mkdir -p /app/monitoring/prometheus /app/monitoring/grafana

# Копируем файлы проекта
COPY ./app/processing_service/main.py /app/
COPY ./app/common /app/common
COPY ./app/processing_service/monitoring /app/monitoring
COPY ./app/processing_service/sagas /app/sagas
COPY ./app/processing_service/core /app/core

# Для отладки - выводим список установленных пакетов
RUN pip list

# Экспозиция портов для Prometheus
EXPOSE 8000

USER appuser

CMD ["python", "main.py"]

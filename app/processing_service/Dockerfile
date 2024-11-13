FROM fsa-base:latest

USER root

WORKDIR /app

# Копируем и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app/processing_service/main.py /app
COPY ./app/common /app/common
COPY ./app/wait-for-it.sh /app

RUN chmod +x /app/wait-for-it.sh
RUN chown -R appuser:appgroup /app

USER appuser

CMD ["python", "main.py"]

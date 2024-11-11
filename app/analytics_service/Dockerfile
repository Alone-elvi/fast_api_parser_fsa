FROM python:3.12-slim
WORKDIR /app
COPY ./app/common /app/common
COPY ./app/analytics_service/main.py /app
COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
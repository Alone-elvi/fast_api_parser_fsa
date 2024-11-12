FROM python:3.12-slim

# Устанавливаем необходимые системные пакеты
RUN apt-get update && apt-get install -y netcat-openbsd gcc libssl-dev libffi-dev && rm -rf /var/lib/apt/lists/*

# Создаем пользователя и группу с переданными UID и GID
ARG USER_ID=1000
ARG GROUP_ID=1000

RUN groupadd -g ${GROUP_ID} appgroup && \
    useradd -m -u ${USER_ID} -g appgroup appuser

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы приложения и модуль common
COPY ./app/processing_service/main.py /app
COPY ./app/common /app/common
COPY ./requirements.txt /app
COPY ./app/wait-for-it.sh /app

RUN chmod +x /app/wait-for-it.sh

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Меняем владельца рабочей директории на нового пользователя
RUN chown -R appuser:appgroup /app

# Переключаемся на нового пользователя
USER appuser

CMD ["python", "main.py"]

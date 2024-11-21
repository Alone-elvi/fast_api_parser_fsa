from prometheus_client import Counter, Histogram, Gauge
import logging
from functools import wraps
from typing import Any, Callable
import redis
import json
from datetime import datetime
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

redis_client = redis.Redis(host='redis', port=6379, db=0)

def cache(ttl: int = 3600):
    """Декоратор для кэширования результатов функций"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Создаем ключ кэша
            cache_key = f"{func.__name__}:{hash(str(args))}{hash(str(kwargs))}"
            
            # Пробуем получить данные из кэша
            cached_result = redis_client.get(cache_key)
            if cached_result:
                return json.loads(cached_result)
            
            # Если данных нет в кэше, вычисляем результат
            result = func(*args, **kwargs)
            
            # Сохраняем результат в кэш
            try:
                redis_client.setex(
                    cache_key,
                    ttl,
                    json.dumps(result, default=str)
                )
            except Exception as e:
                logger.error(f"Cache error: {str(e)}")
            
            return result
        return wrapper
    return decorator

class MetricsCollector:
    def __init__(self):
        # Метрики для полнотекстового поиска
        self.search_requests = Counter(
            'certificate_search_requests_total',
            'Total number of search requests',
            ['status']  # Добавляем статус (success/error)
        )
        self.search_duration = Histogram(
            'certificate_search_duration_seconds',
            'Search request duration in seconds',
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0]  # Определяем конкретные бакеты
        )
        
        # Метрики для истории изменений
        self.history_changes = Counter(
            'certificate_history_changes_total',
            'Total number of certificate changes',
            ['field_name', 'change_type']  # Добавляем тип изменения
        )
        
        # Метрики производительности БД
        self.db_operation_duration = Histogram(
            'database_operation_duration_seconds',
            'Database operation duration in seconds',
            ['operation', 'status'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )

        # Метрики состояния системы
        self.active_sagas = Gauge(
            'active_sagas',
            'Number of currently active sagas'
        )
        
        # Метрики обработки сертификатов
        self.certificates_processed = Counter(
            'certificates_processed_total',
            'Total number of processed certificates',
            ['status']  # success/error/duplicate
        )
        
        # Метрики валидации
        self.validation_errors = Counter(
            'validation_errors_total',
            'Total number of validation errors',
            ['field', 'error_type']
        )

        # Метрики кэша
        self.cache_operations = Counter(
            'cache_operations_total',
            'Total number of cache operations',
            ['operation', 'status']  # hit/miss/error
        )
        
        # Метрики очереди
        self.queue_size = Gauge(
            'processing_queue_size',
            'Current size of the processing queue'
        )
        
        # Метрики производительности API
        self.api_requests = Counter(
            'api_requests_total',
            'Total number of API requests',
            ['endpoint', 'method', 'status_code']
        )
        self.api_latency = Summary(
            'api_request_latency_seconds',
            'API request latency in seconds',
            ['endpoint']
        )

    def record_search(self, duration: float, status: str = 'success') -> None:
        """Записать метрики поиска"""
        self.search_requests.labels(status=status).inc()
        self.search_duration.observe(duration)

    def record_db_operation(self, operation: str, duration: float, status: str = 'success') -> None:
        """Записать метрики операции с БД"""
        self.db_operation_duration.labels(
            operation=operation,
            status=status
        ).observe(duration)

    def record_certificate_processed(self, status: str = 'success') -> None:
        """Записать метрики обработки сертификата"""
        self.certificates_processed.labels(status=status).inc()

    def record_validation_error(self, field: str, error_type: str) -> None:
        """Записать ошибку валидации"""
        self.validation_errors.labels(
            field=field,
            error_type=error_type
        ).inc()

    def record_cache_operation(self, operation: str, status: str) -> None:
        """Записать операцию с кэшем"""
        self.cache_operations.labels(
            operation=operation,
            status=status
        ).inc()

    def update_queue_size(self, size: int) -> None:
        """Обновить размер очереди"""
        self.queue_size.set(size)

    def record_api_request(self, endpoint: str, method: str, status_code: int, duration: float) -> None:
        """Записать метрики API запроса"""
        self.api_requests.labels(
            endpoint=endpoint,
            method=method,
            status_code=status_code
        ).inc()
        self.api_latency.labels(endpoint=endpoint).observe(duration)
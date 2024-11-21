# processing_service/main.py
from confluent_kafka import Consumer, KafkaError
import json
import logging
from datetime import datetime
import hashlib
from typing import Dict, Any, Optional, List, Tuple
import psycopg2
from psycopg2.extras import execute_batch
from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    POSTGRES_DSN,
    CLICKHOUSE_DSN,
    KAFKA_TOPIC_VALIDATED_CERTIFICATES
)
from common.clickhouse import ClickhouseClient
import time
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
from contextlib import contextmanager
from sagas.certificate_saga import CertificateSaga
from monitoring.metrics import MetricsCollector
from prometheus_client import start_http_server
import threading
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Класс для управления подключением к БД"""
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    @property
    def conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        return self._conn

    def _execute_query(self, query: str, params: Any = None) -> Optional[List[Tuple]]:
        """Выполнение SQL запроса с обработкой ошибок"""
        with self.cursor() as cur:
            try:
                logger.info(f"Executing query: {query}")
                logger.info(f"With params: {params}")
                
                cur.execute(query, params)
                self.conn.commit()
                
                if cur.description:  # Если есть результаты
                    result = cur.fetchall()
                    logger.info(f"Query result: {result}")
                    return result
                return None
                
            except psycopg2.Error as e:
                self.conn.rollback()
                logger.error(f"Database error: {e.pgerror}")
                raise
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Unexpected error: {e}")
                raise

    def _execute_query_async(self, query: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Асинхронное выполнение SQL запроса"""
        with self.cursor() as cur:
            try:
                cur.execute(query, params or {})
                self.conn.commit()
                try:
                    return cur.fetchone()
                except psycopg2.ProgrammingError:
                    return None
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Database query error: {e}")
                raise

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def cursor(self):
        """Контекстный менеджер для работы с курсором"""
        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


class CertificateProcessor:
    def __init__(self, db: DatabaseConnection):
        self.db = db
        self.saga_manager = CertificateSaga(self.db)

    def process_message(self, message_data: Dict[str, Any]) -> None:
        """Обработка сообщения из Kafka"""
        try:
            logger.info("=== Starting message processing ===")
            if not isinstance(message_data, dict):
                logger.error(f"Invalid message format: {message_data}")
                return

            records = message_data.get('records', [])
            if not records:
                logger.error("No records found in message")
                return

            for record in records:
                logger.info("=== Processing record ===")
                logger.info(f"Product group in record: {record.get('product_group')}")
                logger.info(f"Product name in record: {record.get('product_name')}")
                
                processed_record = self.process_record(record)
                if processed_record:
                    logger.info(f"Product group after processing: {processed_record.get('product_group')}")
                    self.saga_manager.execute(processed_record)
                else:
                    logger.error(f"Failed to process record: {record}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def _process_single_record(self, record: Dict[str, Any], file_id: str) -> None:
        """Обрабатывает одну запись"""
        try:
            success = self.saga_manager.execute(record)
            if not success:
                logger.error(f"Failed to process record: {record.get('cert_number')}")
        except Exception as e:
            logger.error(f"Error in process_single_record: {e}")
            raise

    def delete_record(self, table: str, record_id: int) -> None:
        """Универсальный метод удаления записи"""
        if record_id:
            self._execute_query(f"DELETE FROM {table} WHERE id = %s", (record_id,))

    def delete_manufacturer(self, state: Dict[str, Any]) -> None:
        """Компенсационная операция для производителя"""
        self.delete_record("manufacturers", state.get('manufacturer_id'))

    def delete_applicant(self, state: Dict[str, Any]) -> None:
        """Компенсационная операция для заявителя"""
        self.delete_record("applicants", state.get('applicant_id'))

    def _create_saga_steps(self, processed_data: Dict[str, Any], file_id: str) -> List[Dict[str, Any]]:
        """Создание шагов SAGA"""
        cert_data = {**processed_data['certificate'], 'file_id': file_id}
        
        return [
            {
                'action': lambda state: {'manufacturer_id': self.save_manufacturer(processed_data['manufacturer'])},
                'compensation': self.delete_manufacturer
            },
            {
                'action': lambda state: {'applicant_id': self.save_applicant(processed_data['applicant'])},
                'compensation': self.delete_applicant
            },
            {
                'action': lambda state: {'cert_body_id': self.save_certification_body(processed_data['certification_body'])},
                'compensation': lambda state: self.delete_record("certification_bodies", state['cert_body_id'])
            },
            {
                'action': lambda state: {
                    'certificate_id': self.save_certificate({
                        **cert_data,
                        'manufacturer_id': state['manufacturer_id'],
                        'applicant_id': state['applicant_id'],
                        'certification_body_id': state['cert_body_id']
                    })
                },
                'compensation': lambda state: self.delete_record("certificates", state['certificate_id'])
            }
        ]

    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Обработка одной записи"""
        try:
            logger.info("=== Processing new record ===")
            logger.info(f"Record data: {record}")
            
            processed_data = {
                'cert_number': record.get('cert_number'),
                'cert_status': record.get('cert_status'),
                'certification_scheme': record.get('certification_scheme'),
                'product_name': record.get('product_name'),
                'product_group': record.get('product_group'),
                'cert_registration_date': record.get('cert_registration_date'),
                'cert_expiration_date': record.get('cert_expiration_date'),
                'cert_termination_date': record.get('cert_termination_date'),
                'termination_reason': record.get('termination_reason'),
                
                # Данные производителя
                'manufacturer_name': record.get('manufacturer_name'),
                'manufacturer_inn': record.get('manufacturer_inn'),
                'manufacturer_address': record.get('manufacturer_address'),
                
                # Данные заявителя
                'applicant_name': record.get('applicant_name'),
                'applicant_type': record.get('applicant_type'),
                'applicant_inn': record.get('applicant_inn'),
                'applicant_ogrn': record.get('applicant_ogrn'),
                'applicant_address': record.get('applicant_address'),
                
                # Данные органа сертификации
                'certification_body_name': record.get('certification_body_name'),
                'certification_body_number': record.get('certification_body_number'),
                'certification_body_ogrn': record.get('certification_body_ogrn'),
                'certification_body_accreditation_status': record.get('certification_body_accreditation_status'),
                'certification_body_accreditation_end_date': record.get('certification_body_accreditation_end_date')
            }
            
            logger.info(f"Processed data: {processed_data}")
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return None

    def _map_manufacturer_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'name': data.get('manufacturer_name', 'Не указано'),
            'address': data.get('manufacturer_address', 'Не указано'),
            'inn': data.get('manufacturer_inn', 'Не указано'),
            'ogrn': data.get('manufacturer_ogrn', 'Не указано')
        }

    def _map_applicant_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'address': data.get('applicant_address'),
            'inn': data.get('applicant_inn', 'Не указано'),
            'ogrn': data.get('applicant_ogrn', 'Не указано'),
            'full_name': data.get('applicant_name') or data.get('applicant_address', 'Не указано'),
            'type': data.get('applicant_type')
        }

    def _map_certification_body_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'certification_body_number': data.get('certification_body_number', 'Не указано'),
            'certification_body_name': data.get('certification_body_name', 'Не указано'),
            'certification_body_accreditation_status': data.get('certification_body_accreditation_status', 'Не указано'),
            'certification_body_accreditation_end_date': data.get('certification_body_accreditation_end_date')
        }

    def _map_certificate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Маппинг данных сертификата"""
        return {
            'cert_number': data.get('cert_number', 'Не указано'),
            'certification_scheme': data.get('certification_scheme', 'Не указано'),
            'cert_status': data.get('cert_status', 'Не указано'),
            'product_name': data.get('product_name', 'Не указано'),
            'cert_registration_date': data.get('cert_registration_date'),
            'cert_expiration_date': data.get('cert_expiration_date'),
            'cert_termination_date': data.get('cert_termination_date'),
            'manufacturer_id': None,
            'applicant_id': None,
            'certification_body_id': None,
            'file_id': None
        }

    def parse_date(self, date_str: str) -> Optional[datetime.date]:
        """Парсинг даты из строки"""
        if not date_str:
            return None
        try:
            if isinstance(date_str, str):
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            return date_str
        except Exception as e:
            logger.error(f"Error parsing date {date_str}: {e}")
            return None

    def save_manufacturer(self, manufacturer_data: Dict[str, Any]) -> int:
        """Сохранение данных производителя"""
        try:
            result = self._execute_query("""
                INSERT INTO manufacturers (
                    manufacturer_name,
                    manufacturer_inn,
                    manufacturer_address
                ) VALUES (
                    %(manufacturer_name)s,
                    %(manufacturer_inn)s,
                    %(manufacturer_address)s
                )
                ON CONFLICT (manufacturer_inn) 
                DO UPDATE SET 
                    manufacturer_name = EXCLUDED.manufacturer_name,
                    manufacturer_address = EXCLUDED.manufacturer_address,
                    updated_at = NOW()
                RETURNING id;
            """, manufacturer_data)
            
            if not result:
                logger.error("No result from manufacturer query")
                return None
            
            manufacturer_id = result[0][0]
            logger.info(f"Manufacturer saved with ID: {manufacturer_id}")
            return manufacturer_id
            
        except Exception as e:
            logger.error(f"Error saving manufacturer: {e}")
            logger.error(f"Data: {manufacturer_data}")
            raise

    def save_applicant(self, applicant_data: Dict[str, Any]) -> int:
        """Сохранение данных заявителя"""
        # Если ИНН не указан или равен 'Не указано', генерируем уникальный идентификатор
        if not applicant_data.get('applicant_inn') or applicant_data['applicant_inn'] == 'Не указано':
            applicant_data['applicant_inn'] = f"UNKNOWN_{uuid.uuid4().hex[:10]}"
        
        try:
            result = self._execute_query("""
                INSERT INTO applicants (
                    applicant_name,
                    applicant_type,
                    applicant_inn,
                    applicant_ogrn,
                    applicant_address
                ) VALUES (
                    %(applicant_name)s,
                    %(applicant_type)s,
                    %(applicant_inn)s,
                    %(applicant_ogrn)s,
                    %(applicant_address)s
                )
                ON CONFLICT (applicant_inn) 
                DO UPDATE SET 
                    applicant_name = EXCLUDED.applicant_name,
                    applicant_type = EXCLUDED.applicant_type,
                    applicant_ogrn = EXCLUDED.applicant_ogrn,
                    applicant_address = EXCLUDED.applicant_address,
                    updated_at = NOW()
                RETURNING id;
            """, applicant_data)
            
            if not result:
                logger.error("No result from applicant query")
                return None
            
            applicant_id = result[0][0]
            logger.info(f"Applicant saved with ID: {applicant_id}")
            return applicant_id
            
        except Exception as e:
            logger.error(f"Error saving applicant: {e}")
            logger.error(f"Data: {applicant_data}")
            raise

    def save_certification_body(self, cert_body_data: Dict[str, Any]) -> int:
        """Сохранение данных органа сертификации"""
        result = self._execute_query("""
            INSERT INTO certification_bodies (
                ral_number,
                full_name,
                ogrn,
                accreditation_status,
                accreditation_end_date
            ) VALUES (
                %(certification_body_number)s,
                %(certification_body_name)s,
                %(certification_body_ogrn)s,
                %(certification_body_accreditation_status)s,
                %(certification_body_accreditation_end_date)s
            )
            ON CONFLICT (ral_number) 
            DO UPDATE SET 
                full_name = EXCLUDED.full_name,
                ogrn = EXCLUDED.ogrn,
                accreditation_status = EXCLUDED.accreditation_status,
                accreditation_end_date = EXCLUDED.accreditation_end_date,
                updated_at = NOW()
            RETURNING id;
        """, cert_body_data)
        
        return result[0] if result else None

    def calculate_certificate_hash(self, cert_data: Dict[str, Any]) -> str:
        """Создает SHA-256 хеш из данных сертификата"""
        hash_fields = {
            'registration_number': cert_data.get('cert_number'),
            'product_name': cert_data.get('product_name'),
            'registration_date': str(cert_data.get('cert_registration_date')),
            'expiration_date': str(cert_data.get('cert_expiration_date')),
            'status': cert_data.get('cert_status')
        }
        
        hash_string = '|'.join(str(value) for value in sorted(hash_fields.values()) if value)
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

    def check_certificate_hash(self, hash_value: str) -> Optional[int]:
        """Проверяе существование хеша в БД"""
        result = self._execute_query("""
            SELECT certificate_id 
            FROM certificate_hashes 
            WHERE hash_value = %s
        """, (hash_value,))
        return result[0] if result else None

    def save_certificate_hash(self, certificate_id: int, cert_hash: str) -> None:
        """Сохранение хеша сертификата с проверкой на дубликаты"""
        try:
            self._execute_query("""
                INSERT INTO certificate_hashes (certificate_id, hash_value, created_at)
                VALUES (%(certificate_id)s, %(hash_value)s, NOW())
                ON CONFLICT (certificate_id) 
                DO UPDATE SET 
                    hash_value = EXCLUDED.hash_value,
                    created_at = NOW()
            """, {
                'certificate_id': certificate_id,
                'hash_value': cert_hash
            })
            logger.info(f"Hash saved/updated for certificate ID: {certificate_id}")
        except Exception as e:
            logger.error(f"Error saving certificate hash: {e}", exc_info=True)
            raise

    def search_certificates(self, query: str) -> List[Dict[str, Any]]:
        """Полнотекстовый поиск по сертификатам"""
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    SELECT c.*, 
                           ts_rank(c.search_vector, plainto_tsquery('russian', %s)) as rank
                    FROM certificates c
                    WHERE c.search_vector @@ plainto_tsquery('russian', %s)
                    ORDER BY rank DESC
                    LIMIT 100
                """, (query, query))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
                
        except Exception as e:
            logger.error(f"Error in search_certificates: {str(e)}")
            raise

    def get_certificate_history(self, certificate_id: int) -> List[Dict[str, Any]]:
        """Получение истории изменений сертификата"""
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    SELECT field_name, old_value, new_value, changed_at, changed_by
                    FROM certificates_history
                    WHERE certificate_id = %s
                    ORDER BY changed_at DESC
                """, (certificate_id,))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
                
        except Exception as e:
            logger.error(f"Error in get_certificate_history: {str(e)}")
            raise

    def check_certificate_exists(self, cert_data: Dict[str, Any]) -> Optional[int]:
        """Проверка существования сертификата"""
        try:
            result = self._execute_query("""
                SELECT id 
                FROM certificates 
                WHERE registration_number = %s
            """, (cert_data.get('cert_number'),))
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise

    def save_certificate(self, cert_data: Dict[str, Any]) -> int:
        """Сохранение сертификата"""
        if not cert_data.get("manufacturer_id"):
            raise ValueError("manufacturer_id is missing for certificate data")
        
        try:
            result = self._execute_query("""
                INSERT INTO certificates (
                    cert_number,
                    product_group,
                    certification_scheme,
                    cert_status,
                    product_name,
                    cert_registration_date,
                    cert_expiration_date,
                    cert_termination_date,
                    termination_reason,
                    manufacturer_id,
                    applicant_id,
                    certification_body_id,
                    file_id
                ) VALUES (
                    %(cert_number)s,
                    %(product_group)s,
                    %(certification_scheme)s,
                    %(cert_status)s,
                    %(product_name)s,
                    %(cert_registration_date)s,
                    %(cert_expiration_date)s,
                    %(cert_termination_date)s,
                    %(termination_reason)s,
                    %(manufacturer_id)s,
                    %(applicant_id)s,
                    %(certification_body_id)s,
                    %(file_id)s
                )
                ON CONFLICT (cert_number) 
                DO UPDATE SET
                    product_group = EXCLUDED.product_group,
                    certification_scheme = EXCLUDED.certification_scheme,
                    cert_status = EXCLUDED.cert_status,
                    product_name = EXCLUDED.product_name,
                    cert_registration_date = EXCLUDED.cert_registration_date,
                    cert_expiration_date = EXCLUDED.cert_expiration_date,
                    cert_termination_date = EXCLUDED.cert_termination_date,
                    termination_reason = EXCLUDED.termination_reason,
                    manufacturer_id = EXCLUDED.manufacturer_id,
                    applicant_id = EXCLUDED.applicant_id,
                    certification_body_id = EXCLUDED.certification_body_id,
                    file_id = EXCLUDED.file_id,
                    updated_at = NOW()
                RETURNING id
            """, cert_data)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise


def start_metrics_server():
    start_http_server(8000)

def init_database(db: DatabaseConnection):
    """Инициализация базы данных"""
    try:
        with db.cursor() as cur:
            # Проверяем существование таблицы
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'operation_logs'
                );
            """)
            exists = cur.fetchone()[0]
            
            if not exists:
                # Создаем таблицу если её нет
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS operation_logs (
                        id SERIAL PRIMARY KEY,
                        operation_type VARCHAR(50) NOT NULL,
                        entity_type VARCHAR(50) NOT NULL,
                        entity_id INTEGER NOT NULL,
                        details JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                logger.info("Created operation_logs table")

        # Заполняем хеши для существующих сертификатов
        fill_certificate_hashes(db)
        
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

def fill_certificate_hashes(db: DatabaseConnection) -> None:
    """Заполняет хеши для существующих сертификатов"""
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, cert_number, product_name, cert_registration_date, 
                   cert_expiration_date, cert_status 
            FROM certificates 
            WHERE id NOT IN (SELECT certificate_id FROM certificate_hashes)
        """)
        certificates = cur.fetchall()
        
        for cert in certificates:
            cert_data = {
                'cert_number': cert[1],
                'product_name': cert[2],
                'cert_registration_date': cert[3],
                'cert_expiration_date': cert[4],
                'cert_status': cert[5]
            }
            
            hash_value = db.calculate_certificate_hash(cert_data)
            db.save_certificate_hash(cert[0], hash_value)

def main():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'fsa_processing_service',
        'auto.offset.reset': 'earliest'
    }

    consumer = None
    db = DatabaseConnection(POSTGRES_DSN)
    init_database(db)
    processor = CertificateProcessor(db)

    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC_VALIDATED_CERTIFICATES])
        logger.info("Started consuming messages...")

        # Запускаем сервер метрик в отдельном потоке
        metrics_thread = threading.Thread(target=start_metrics_server)
        metrics_thread.start()

        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    raw_data = msg.value().decode('utf-8')
                    logger.info(f"Received raw message: {raw_data[:500]}")
                    
                    data = json.loads(raw_data)
                    logger.info(f"Parsed JSON data: {str(data)[:500]}")
                    
                    processor.process_message(data)
                    consumer.commit()
                    logger.info("Message processed and committed")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    consumer.commit()
                except Exception as e:
                    logger.error(f"Processing error: {e}", exc_info=True)
                    
            except Exception as e:
                logger.error(f"Consumer loop error: {e}", exc_info=True)
                time.sleep(5)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if consumer:
            consumer.close()
        db.close()


if __name__ == "__main__":
    main()

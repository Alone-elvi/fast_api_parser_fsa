# processing_service/main.py
from confluent_kafka import Consumer, KafkaError
import json
import logging
from datetime import datetime
import hashlib
from typing import Dict, Any, Optional
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    @property
    def conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        return self._conn

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

class CertificateProcessor:
    def __init__(self, db: DatabaseManager):
        self.db = db

    def process_record(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обработка и маппинг данных из входящего сообщения.
        
        Args:
            data: Входящие данные
            
        Returns:
            Dict[str, Any]: Обработанные данные
        """
        try:
            logger.info(f"Raw incoming data: {data}")
            
            # Маппинг данных производителя
            manufacturer_data = {
                'name': data.get('manufacturer_name') or data.get('manufacturer_address', 'Не указано'),
                'address': data.get('manufacturer_address', 'Не указано'),
                'inn': data.get('manufacturer_inn', 'Не указано'),
                'ogrn': data.get('manufacturer_ogrn', 'Не указано')
            }
            
            # Маппинг данных заявителя
            applicant_data = {
                'address': data.get('applicant_address'),
                'inn': data.get('applicant_inn', 'Не указано'),
                'ogrn': data.get('applicant_ogrn', 'Не указано'),
                'full_name': data.get('applicant_name') or data.get('applicant_address', 'Не указано'),
                'type': data.get('applicant_type')
            }

            # Маппинг данных органа сертификации
            certification_body_data = {
                'ral_number': data.get('certification_body_number'),
                'full_name': data.get('certification_body_name'),
                'ogrn': data.get('certification_body_ogrn', 'Не указано'),
                'accreditation_status': data.get('certification_body_accreditation_status'),
                'accreditation_end_date': self.parse_date(data.get('certification_body_accreditation_end_date'))
            }

            # Маппинг данных сертификата
            certificate_data = {
                'registration_number': data.get('cert_number'),
                'eaeu_product_group': data.get('eaeu_product_group', 'Не указано'),
                'certification_scheme': data.get('certification_scheme'),
                'status': data.get('cert_status'),
                'product_name': data.get('product_name'),
                'registration_date': self.parse_date(data.get('cert_registration_date')),
                'expiration_date': self.parse_date(data.get('cert_expiration_date')),
                'termination_date': self.parse_date(data.get('cert_termination_date')),
                'termination_reason': data.get('cert_termination_reason', 'Не указано')
            }

            processed_data = {
                'manufacturer': manufacturer_data,
                'applicant': applicant_data,
                'certification_body': certification_body_data,
                'certificate': certificate_data
            }

            logger.info(f"Processed data: {processed_data}")
            return processed_data

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
            raise

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
        """
        Сохранение данных производителя в БД.
        
        Args:
            manufacturer_data: Словарь с данными производителя
            
        Returns:
            int: ID сохраненного производителя
        """
        try:
            with self.db.conn.cursor() as cur:
                # Используем name из manufacturer_data
                manufacturer_name = manufacturer_data.get('name')
                if not manufacturer_name:
                    logger.warning("Manufacturer name is empty, using default value")
                    manufacturer_name = 'Не указано'
                    
                cur.execute("""
                    INSERT INTO manufacturers (
                        name,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, NOW(), NOW())
                    ON CONFLICT (name) 
                    DO UPDATE SET
                        updated_at = NOW()
                    RETURNING id
                """, (manufacturer_name,))
                
                result = cur.fetchone()
                if result:
                    logger.info(f"Manufacturer saved/updated with id: {result[0]}")
                    return result[0]
                else:
                    logger.error("Failed to save manufacturer")
                    raise Exception("Failed to save manufacturer")
                    
        except Exception as e:
            logger.error(f"Error in save_manufacturer: {str(e)}")
            raise

    def save_applicant(self, applicant_data: Dict[str, Any]) -> int:
        """Сохранение данных заявителя"""
        with self.db.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO applicants (address, inn, ogrn, full_name, type)
                VALUES (%(address)s, %(inn)s, %(ogrn)s, %(full_name)s, %(type)s)
                ON CONFLICT (address, full_name) 
                DO UPDATE SET 
                    inn = EXCLUDED.inn,
                    ogrn = EXCLUDED.ogrn,
                    type = EXCLUDED.type
                RETURNING id;
            """, applicant_data)
            
            result = cur.fetchone()
            return result[0] if result else None

    def save_certification_body(self, cert_body_data: Dict[str, Any]) -> int:
        """Сохранение данных органа сертификации"""
        with self.db.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO certification_bodies 
                    (ral_number, full_name, ogrn, accreditation_status, accreditation_end_date)
                VALUES (
                    %(ral_number)s, %(full_name)s, %(ogrn)s, 
                    %(accreditation_status)s, %(accreditation_end_date)s
                )
                ON CONFLICT (ral_number) 
                DO UPDATE SET 
                    full_name = EXCLUDED.full_name,
                    ogrn = EXCLUDED.ogrn,
                    accreditation_status = EXCLUDED.accreditation_status,
                    accreditation_end_date = EXCLUDED.accreditation_end_date
                RETURNING id;
            """, cert_body_data)
            
            result = cur.fetchone()
            return result[0] if result else None

    def calculate_certificate_hash(self, cert_data: Dict[str, Any]) -> str:
        """
        Создает SHA-256 хеш из данных сертификата.
        
        Args:
            cert_data: Данные сертификата
            
        Returns:
            str: Хеш значение
        """
        # Выбираем поля для хеширования
        hash_fields = {
            'registration_number': cert_data.get('registration_number'),
            'product_name': cert_data.get('product_name'),
            'registration_date': str(cert_data.get('registration_date')),
            'expiration_date': str(cert_data.get('expiration_date')),
            'status': cert_data.get('status')
        }
        
        # Создаем отсортированную строку из значений
        hash_string = '|'.join(str(value) for value in sorted(hash_fields.values()) if value)
        
        # Создаем хеш
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

    def check_certificate_hash(self, hash_value: str) -> Optional[int]:
        """
        Проверяет существование хеша в БД.
        
        Args:
            hash_value: Хеш для проверки
            
        Returns:
            Optional[int]: ID сертификата если хеш найден, иначе None
        """
        with self.db.conn.cursor() as cur:
            cur.execute("""
                SELECT certificate_id 
                FROM certificate_hashes 
                WHERE hash_value = %s
            """, (hash_value,))
            result = cur.fetchone()
            return result[0] if result else None

    def save_certificate_hash(self, certificate_id: int, hash_value: str) -> None:
        """
        Сохраняет хеш сертификата в БД.
        
        Args:
            certificate_id: ID сертификата
            hash_value: Значение хеша
        """
        try:
            logger.info(f"Attempting to save hash for certificate ID {certificate_id}")
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO certificate_hashes (
                        certificate_id, 
                        hash_value,
                        created_at
                    )
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (hash_value) DO NOTHING
                    RETURNING id
                """, (certificate_id, hash_value))
                
                result = cur.fetchone()
                if result:
                    logger.info(f"Hash saved successfully with ID: {result[0]}")
                else:
                    logger.warning(f"Hash was not saved (possibly duplicate): {hash_value}")
                
                # Важно! Добавляем явный commit
                self.db.conn.commit()
                
        except Exception as e:
            logger.error(f"Error saving certificate hash: {e}", exc_info=True)
            self.db.conn.rollback()
            raise

    def save_certificate(self, cert_data: Dict[str, Any]) -> Optional[int]:
        """Сохранение данных сертификата"""
        try:
            # Вычисляем хеш сертификата
            cert_hash = self.calculate_certificate_hash(cert_data)
            logger.info(f"Calculated hash for certificate {cert_data.get('registration_number')}: {cert_hash}")
            
            # Проверяем существование сертификата по хешу
            existing_cert_id = self.check_certificate_hash(cert_hash)
            if existing_cert_id:
                logger.info(f"Certificate with hash {cert_hash} already exists (ID: {existing_cert_id})")
                return existing_cert_id

            logger.info("Saving new certificate to database...")
            # Если сертификат новый - сохраняем его
            with self.db.conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO certificates (
                            registration_number, eaeu_product_group, certification_scheme,
                            status, product_name, registration_date, expiration_date,
                            termination_date, termination_reason, manufacturer_id,
                            applicant_id, certification_body_id, file_id
                        ) VALUES (
                            %(registration_number)s, %(eaeu_product_group)s, %(certification_scheme)s,
                            %(status)s, %(product_name)s, %(registration_date)s, %(expiration_date)s,
                            %(termination_date)s, %(termination_reason)s, %(manufacturer_id)s,
                            %(applicant_id)s, %(certification_body_id)s, %(file_id)s
                        )
                        ON CONFLICT (registration_number) 
                        DO UPDATE SET 
                            eaeu_product_group = EXCLUDED.eaeu_product_group,
                            certification_scheme = EXCLUDED.certification_scheme,
                            status = EXCLUDED.status,
                            product_name = EXCLUDED.product_name,
                            registration_date = EXCLUDED.registration_date,
                            expiration_date = EXCLUDED.expiration_date,
                            termination_date = EXCLUDED.termination_date,
                            termination_reason = EXCLUDED.termination_reason,
                            manufacturer_id = EXCLUDED.manufacturer_id,
                            applicant_id = EXCLUDED.applicant_id,
                            certification_body_id = EXCLUDED.certification_body_id,
                            file_id = EXCLUDED.file_id,
                            updated_at = NOW()
                        RETURNING id;
                    """, cert_data)
                    
                    result = cur.fetchone()
                    if result:
                        certificate_id = result[0]
                        logger.info(f"Certificate saved successfully with ID: {certificate_id}")
                        
                        # Сохраняем хеш
                        try:
                            self.save_certificate_hash(certificate_id, cert_hash)
                            logger.info(f"Hash saved successfully for certificate ID: {certificate_id}, hash: {cert_hash}")
                        except Exception as hash_error:
                            logger.error(f"Error saving hash: {hash_error}", exc_info=True)
                            # Продолжаем выполнение, так как сертификат уже сохранен
                        
                        return certificate_id
                    else:
                        logger.error("Failed to save certificate - no ID returned")
                        return None

                except Exception as db_error:
                    logger.error(f"Database error while saving certificate: {db_error}", exc_info=True)
                    raise

        except Exception as e:
            logger.error(f"Error in save_certificate: {e}", exc_info=True)
            raise

    def process_message(self, message_data: Dict[str, Any]) -> None:
        """Обработка сообщения из Kafka"""
        try:
            logger.info(f"Processing message with file_id: {message_data.get('file_id')}")
            
            # Проверяем наличие записей
            if not message_data.get('records'):
                logger.warning("No records found in message")
                return
                
            # Обрабатываем каждую запись
            for record in message_data['records']:
                try:
                    # Обработка одной записи
                    processed_data = self.process_record(record)
                    
                    # Сохраняем данные в БД
                    with self.db.conn.cursor() as cur:
                        # Сохраняем производителя
                        manufacturer_id = self.save_manufacturer(processed_data['manufacturer'])
                        
                        # Сохраняем заявителя
                        applicant_id = self.save_applicant(processed_data['applicant'])
                        
                        # Сохраняем орган сертификации
                        cert_body_id = self.save_certification_body(processed_data['certification_body'])
                        
                        # Сохраняем сертификат
                        cert_data = processed_data['certificate']
                        cert_data.update({
                            'manufacturer_id': manufacturer_id,
                            'applicant_id': applicant_id,
                            'certification_body_id': cert_body_id,
                            'file_id': message_data.get('file_id')
                        })
                        cert_id = self.save_certificate(cert_data)
                        
                        self.db.conn.commit()
                        logger.info(f"Successfully processed record: {cert_data['registration_number']}")
                        
                except Exception as e:
                    logger.error(f"Error processing record: {e}", exc_info=True)
                    self.db.conn.rollback()
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            raise

def main():
    # Настройка Kafka Consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'fsa_processing_service',
        'auto.offset.reset': 'earliest'
    }

    consumer = None
    db = DatabaseManager(POSTGRES_DSN)
    processor = CertificateProcessor(db)

    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC_VALIDATED_CERTIFICATES])
        logger.info("Started consuming messages...")

        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Декодируем сообщение
                    raw_data = msg.value().decode('utf-8')
                    logger.info(f"Received raw message: {raw_data[:500]}")
                    
                    data = json.loads(raw_data)
                    logger.info(f"Parsed JSON data: {str(data)[:500]}")
                    
                    # Обработка сообщения
                    processor.process_message(data)
                    
                    # Коммитим смещение
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

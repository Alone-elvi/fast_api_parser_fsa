from typing import Dict, Any, Optional
from datetime import datetime
from core.saga import Saga, SagaStep
from monitoring.metrics import MetricsCollector
import logging
import json
import hashlib

logger = logging.getLogger(__name__)


class CertificateSaga(Saga):
    def __init__(self, db_service):
        super().__init__()
        self.db_service = db_service
        self.certificate_id = None
        self.cert_hash = None
        self.metrics = MetricsCollector()
        self.start_time = None

        # Добавляем шаги с логированием
        self.add_step(SagaStep(
            action=self.validate_data,
            compensation=self.log_validation_failure
        ))
        self.add_step(SagaStep(
            action=lambda data: not self.check_duplicate(data),
            compensation=self.log_duplicate_check_failure
        ))
        self.add_step(SagaStep(
            action=self.save_manufacturer,
            compensation=self.rollback_manufacturer
        ))
        self.add_step(SagaStep(
            action=self.save_applicant,
            compensation=self.rollback_applicant
        ))
        self.add_step(SagaStep(
            action=self.save_certification_body,
            compensation=self.rollback_certification_body
        ))
        self.add_step(SagaStep(
            action=self.save_certificate,
            compensation=self.rollback_certificate
        ))

    def execute(self, cert_data: Dict[str, Any]) -> bool:
        """Выполнение саги по сохранению сертификата"""
        try:
            logger.info("=== Starting certificate saga ===")
            logger.info(f"Initial data: {cert_data}")

            # 1. Сохраняем производителя
            manufacturer_id = self.save_manufacturer(cert_data)
            if not manufacturer_id:
                logger.error("Failed to save manufacturer")
                return False
            cert_data['manufacturer_id'] = manufacturer_id
            logger.info(f"Saved manufacturer with ID: {manufacturer_id}")

            # 2. Сохраняем заявителя
            applicant_id = self.save_applicant(cert_data)
            if not applicant_id:
                logger.error("Failed to save applicant")
                return False
            cert_data['applicant_id'] = applicant_id
            logger.info(f"Saved applicant with ID: {applicant_id}")

            # 3. Сохраняем орган сертификации
            cert_body_id = self.save_certification_body(cert_data)
            if not cert_body_id:
                logger.error("Failed to save certification body")
                return False
            cert_data['certification_body_id'] = cert_body_id
            logger.info(f"Saved certification body with ID: {cert_body_id}")

            # 4. Проверяем наличие всех необходимых ID
            required_ids = ['manufacturer_id', 'applicant_id', 'certification_body_id']
            missing_ids = [id_field for id_field in required_ids if id_field not in cert_data]
            
            if missing_ids:
                logger.error(f"Missing required IDs: {missing_ids}")
                logger.error(f"Available data: {cert_data}")
                return False

            # 5. Сохраняем сертификат
            certificate_id = self.save_certificate(cert_data)
            if not certificate_id:
                logger.error("Failed to save certificate")
                return False

            logger.info("=== Certificate saga completed successfully ===")
            return True

        except Exception as e:
            logger.error(f"Error in certificate saga: {e}")
            logger.error(f"Data at error: {cert_data}")
            return False

    def validate_data(self, cert_data: Dict[str, Any]) -> bool:
        logger.info("=== Validating data ===")
        logger.info(f"Product_group in validate_data: {cert_data.get('product_group')}")
        try:
            logger.info("=== Validating data ===")
            logger.info(f"Product group in validation: {cert_data.get('product_group')}")
            
            if not isinstance(cert_data, dict):
                logger.error(f"Invalid data type: {type(cert_data)}")
                return False

            required_fields = [
                "cert_number",
                "cert_status",
                "product_name",
                "product_group",
                "cert_registration_date",
                "cert_expiration_date",
                "certification_scheme",
                "applicant_type",
                "applicant_address",
                "manufacturer_address",
                "certification_body_number",
            ]

            missing_fields = [field for field in required_fields if not cert_data.get(field)]
            if missing_fields:
                logger.error(f"Missing required fields: {', '.join(missing_fields)}")
                return False

            return True

        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    def save_manufacturer(self, cert_data: Dict[str, Any]) -> Optional[int]:
        """Сохранение производителя"""
        try:
            logger.info("=== Saving manufacturer ===")
            logger.info(f"Name: {cert_data.get('manufacturer_name')}")
            logger.info(f"INN: {cert_data.get('manufacturer_inn')}")
            
            # Сначала пробуем найти по INN
            result = self.db_service._execute_query("""
                SELECT id FROM manufacturers 
                WHERE manufacturer_inn = %(manufacturer_inn)s;
            """, cert_data)
            
            if result and result[0]:
                # Если нашли - обновляем
                manufacturer_id = result[0][0]
                self.db_service._execute_query("""
                    UPDATE manufacturers SET
                        manufacturer_name = %(manufacturer_name)s,
                        manufacturer_address = %(manufacturer_address)s,
                        updated_at = NOW()
                    WHERE id = %(id)s;
                """, {**cert_data, 'id': manufacturer_id})
                return manufacturer_id
                
            # Если не нашли - создаем новую запись
            result = self.db_service._execute_query("""
                INSERT INTO manufacturers (
                    manufacturer_name,
                    manufacturer_inn,
                    manufacturer_address
                ) VALUES (
                    %(manufacturer_name)s,
                    %(manufacturer_inn)s,
                    %(manufacturer_address)s
                )
                RETURNING id;
            """, cert_data)
            
            if not result or not result[0]:
                logger.error("Failed to insert manufacturer")
                return None
                
            manufacturer_id = result[0][0]
            logger.info(f"=== Manufacturer saved successfully with ID: {manufacturer_id} ===")
            return manufacturer_id
            
        except Exception as e:
            logger.error(f"Error saving manufacturer: {e}")
            logger.error(f"Data: {cert_data}")
            return None

    def save_applicant(self, cert_data: Dict[str, Any]) -> Optional[int]:
        """Сохранение заявителя"""
        try:
            logger.info("=== Saving applicant ===")
            
            # Нормализация данных
            applicant_data = {
                'applicant_name': cert_data.get('applicant_name'),
                'applicant_type': cert_data.get('applicant_type'),
                'applicant_inn': cert_data.get('applicant_inn'),
                'applicant_ogrn': cert_data.get('applicant_ogrn'),
                'applicant_address': cert_data.get('applicant_address')
            }
            
            # Нормализация значений
            for key, value in applicant_data.items():
                if value in ['Не указано', 'Отсутствует', '', None]:
                    applicant_data[key] = None
            
            logger.info(f"Normalized applicant data: {applicant_data}")
            
            # Поиск существующего заявителя
            existing_id = None
            
            # Поиск по ИНН если есть
            if applicant_data['applicant_inn']:
                result = self.db_service._execute_query("""
                    SELECT id FROM applicants 
                    WHERE applicant_inn = %(applicant_inn)s;
                """, applicant_data)
                if result and result[0]:
                    existing_id = result[0][0]
            
            # Если нет ИНН, поиск по ОГРН
            if not existing_id and applicant_data['applicant_ogrn']:
                result = self.db_service._execute_query("""
                    SELECT id FROM applicants 
                    WHERE applicant_ogrn = %(applicant_ogrn)s;
                """, applicant_data)
                if result and result[0]:
                    existing_id = result[0][0]
            
            # Если нашли существующего - обновляем
            if existing_id:
                update_fields = []
                update_values = {'id': existing_id}
                
                for field, value in applicant_data.items():
                    if value is not None:  # Обновляем только непустые значения
                        update_fields.append(f"{field} = %({field})s")
                        update_values[field] = value
                
                if update_fields:
                    self.db_service._execute_query(f"""
                        UPDATE applicants SET
                            {', '.join(update_fields)},
                            updated_at = NOW()
                        WHERE id = %(id)s;
                    """, update_values)
                
                return existing_id
            
            # Если не нашли - создаем новую запись
            result = self.db_service._execute_query("""
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
                RETURNING id;
            """, applicant_data)
            
            if not result or not result[0]:
                logger.error("Failed to insert applicant")
                return None
                
            applicant_id = result[0][0]
            logger.info(f"=== Applicant saved successfully with ID: {applicant_id} ===")
            return applicant_id
            
        except Exception as e:
            logger.error(f"Error saving applicant: {e}")
            logger.error(f"Data: {cert_data}")
            return None

    def save_certification_body(self, cert_data: Dict[str, Any]) -> Optional[int]:
        """Сохранение органа сертификации"""
        try:
            logger.info("=== Saving certification body ===")
            logger.info(f"Number: {cert_data.get('certification_body_number')}")
            
            # Сначала пробуем найти по номеру
            result = self.db_service._execute_query("""
                SELECT id FROM certification_bodies 
                WHERE certification_body_number = %(certification_body_number)s;
            """, cert_data)
            
            if result and result[0]:
                # Если нашли - обновляем
                cert_body_id = result[0][0]
                self.db_service._execute_query("""
                    UPDATE certification_bodies SET
                        certification_body_name = %(certification_body_name)s,
                        certification_body_ogrn = %(certification_body_ogrn)s,
                        certification_body_accreditation_status = %(certification_body_accreditation_status)s,
                        certification_body_accreditation_end_date = %(certification_body_accreditation_end_date)s,
                        updated_at = NOW()
                    WHERE id = %(id)s;
                """, {**cert_data, 'id': cert_body_id})
                cert_data['certification_body_id'] = cert_body_id
                return cert_body_id
                
            # Если не нашли - создаем новую запись
            result = self.db_service._execute_query("""
                INSERT INTO certification_bodies (
                    certification_body_number,
                    certification_body_name,
                    certification_body_ogrn,
                    certification_body_accreditation_status,
                    certification_body_accreditation_end_date
                ) VALUES (
                    %(certification_body_number)s,
                    %(certification_body_name)s,
                    %(certification_body_ogrn)s,
                    %(certification_body_accreditation_status)s,
                    %(certification_body_accreditation_end_date)s
                )
                RETURNING id;
            """, cert_data)
            
            if not result or not result[0]:
                logger.error("Failed to insert certification body")
                return None
                
            cert_body_id = result[0][0]
            cert_data['certification_body_id'] = cert_body_id
            logger.info(f"=== Certification body saved successfully with ID: {cert_body_id} ===")
            return cert_body_id
            
        except Exception as e:
            logger.error(f"Error saving certification body: {e}")
            logger.error(f"Data: {cert_data}")
            return None

    def calculate_certificate_hash(self, cert_data: Dict[str, Any]) -> str:
        """Создает SHA-256 хеш из данных сертификата"""
        hash_fields = {
            "cert_number": cert_data.get("cert_number"),
            "product_name": cert_data.get("product_name"),
            "manufacturer_inn": cert_data.get("manufacturer_inn"),
            "applicant_inn": cert_data.get("applicant_inn"),
            "cert_registration_date": str(cert_data.get("cert_registration_date")),
            "cert_expiration_date": str(cert_data.get("cert_expiration_date")),
        }
        
        # Сортируем ключи для обеспечения постоянного прядка
        hash_string = json.dumps(hash_fields, sort_keys=True)
        logger.info(f"String for hashing: {hash_string}")
        
        return hashlib.sha256(hash_string.encode()).hexdigest()

    def check_duplicate(self, cert_data: Dict[str, Any]) -> bool:
        """Проверяет наличие дубликата сертификата по хешу"""
        try:
            cert_hash = self.calculate_certificate_hash(cert_data)
            logger.info(f"Checking for duplicate with hash: {cert_hash}")
            
            result = self.db_service._execute_query(
                """
                SELECT c.cert_number, c.cert_status
                FROM certificate_hashes ch
                JOIN certificates c ON c.id = ch.certificate_id
                WHERE ch.hash_value = %s
                """,
                (cert_hash,)
            )
            
            if result:
                logger.info(f"Found duplicate certificate: {result}")
                return True
            
            logger.info("No duplicate found")
            return False
            
        except Exception as e:
            logger.error(f"Error checking for duplicate: {e}")
            return False

    def save_certificate(self, cert_data: Dict[str, Any]) -> Optional[int]:
        """Сохранение сертификата"""
        try:
            # Генерируем хеш перед сохранением
            cert_hash = self._generate_certificate_hash(cert_data)
            logger.info(f"Generated hash for certificate: {cert_hash}")
            
            # Проверяем существование по хешу
            hash_result = self.db_service._execute_query("""
                SELECT c.id 
                FROM certificates c
                JOIN certificate_hashes ch ON c.id = ch.certificate_id
                WHERE ch.hash_value = %(hash)s;
            """, {'hash': cert_hash})
            
            if hash_result and hash_result[0]:
                logger.info(f"Certificate with hash {cert_hash} already exists")
                return hash_result[0][0]
            
            # Сохраняем сертификат
            result = self.db_service._execute_query("""
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
                    certification_body_id
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
                    %(certification_body_id)s
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
                    updated_at = NOW()
                RETURNING id;
            """, cert_data)
            
            if not result or not result[0]:
                logger.error("Failed to save certificate")
                return None
            
            certificate_id = result[0][0]
            
            # Сохраняем хеш
            self.db_service._execute_query("""
                INSERT INTO certificate_hashes (
                    certificate_id,
                    hash_value
                ) VALUES (
                    %(certificate_id)s,
                    %(hash_value)s
                );
            """, {
                'certificate_id': certificate_id,
                'hash_value': cert_hash
            })
            
            logger.info(f"Saved certificate with ID {certificate_id} and hash {cert_hash}")
            return certificate_id
            
        except Exception as e:
            logger.error(f"Error saving certificate: {e}")
            logger.error(f"Data: {cert_data}")
            return None

    def _generate_certificate_hash(self, cert_data: Dict[str, Any]) -> str:
        """Генерация хеша сертификата"""
        # Создаем отсортированный список ключевых полей для хеширования
        hash_fields = [
            'cert_number',
            'product_name',
            'product_group',
            'cert_registration_date',
            'cert_expiration_date',
            'manufacturer_name',
            'manufacturer_inn',
            'applicant_name',
            'applicant_inn',
            'certification_body_number'
        ]
        
        # Создаем словарь только с нужными полями
        hash_data = {
            field: str(cert_data.get(field, '')) 
            for field in hash_fields 
            if field in cert_data
        }
        
        # Создаем строку для хеширования
        hash_string = json.dumps(hash_data, sort_keys=True)
        
        # Генерируем SHA-256 хеш
        return hashlib.sha256(hash_string.encode()).hexdigest()

    def update_metrics(self, cert_data: Dict[str, Any]) -> bool:
        """Шаг 5: Обновление метрик"""
        try:
            self.metrics.increment("certificates_processed")
            self.metrics.increment(f"certificates_by_status_{cert_data['cert_status']}")

            duration = (datetime.now() - self.start_time).total_seconds()
            self.metrics.observe("total_processing_duration", duration)

            # Сохраняем детали операции
            self.db_service._execute_query(
                """
                INSERT INTO operation_logs (
                    operation_type, entity_id, duration, cert_status, details
                ) VALUES (
                    'certificate_processing',
                    %s,
                    %s,
                    'success',
                    %s
                )
            """,
                (
                    self.certificate_id,
                    duration,
                    json.dumps(
                        {
                            "cert_number": cert_data["cert_number"],
                            "cert_status": cert_data["cert_status"],
                            "steps_completed": self.completed_steps,
                        }
                    ),
                ),
            )

            return True

        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
            return False

    def log_validation_failure(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для validate_data"""
        logger.error(
            f"Validation failed for certificate: {cert_data.get('cert_number')}"
        )
        self.metrics.increment("validation_failures")
        try:
            self.db_service._execute_query(
                """
                INSERT INTO validation_failures (
                    cert_number, error_type, error_details, created_at
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    cert_data.get("cert_number"),
                    "validation_error",
                    json.dumps(cert_data),
                    datetime.now(),
                ),
            )
        except Exception as e:
            logger.error(f"Failed to log validation failure: {e}")

    def log_duplicate_check_failure(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для check_duplicates"""
        logger.error(
            f"Duplicate check failed for certificate: {cert_data.get('cert_number')}"
        )
        self.metrics.increment("duplicate_check_failures")

    def rollback_certificate(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для save_certificate"""
        if self.certificate_id:
            try:
                self.db_service._execute_query(
                    """
                    DELETE FROM certificates WHERE id = %s
                """,
                    (self.certificate_id,),
                )
                logger.info(f"Rolled back certificate {self.certificate_id}")
            except Exception as e:
                logger.error(f"Failed to rollback certificate: {e}")

    def rollback_hash(self, cert_data: Dict[str, Any]) -> None:
        """омпенсация для save_hash"""
        if self.certificate_id:
            try:
                self.db_service._execute_query(
                    """
                    DELETE FROM certificate_hashes WHERE certificate_id = %s
                """,
                    (self.certificate_id,),
                )
                logger.info(f"Rolled back hash for certificate {self.certificate_id}")
            except Exception as e:
                logger.error(f"Failed to rollback hash: {e}")

    def rollback_metrics(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для update_metrics"""
        try:
            self.metrics.decrement("certificates_processed")
            if cert_data.get("cert_status"):
                self.metrics.decrement(
                    f"certificates_by_status_{cert_data['cert_status']}"
                )
            logger.info("Rolled back metrics")
        except Exception as e:
            logger.error(f"Failed to rollback metrics: {e}")

    def rollback_manufacturer(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для save_manufacturer"""
        if manufacturer_id := cert_data.get('manufacturer_id'):
            try:
                self.db_service._execute_query(
                    "DELETE FROM manufacturers WHERE id = %s",
                    (manufacturer_id,)
                )
            except Exception as e:
                logger.error(f"Failed to rollback manufacturer: {e}")

    def rollback_applicant(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для save_applicant"""
        if applicant_id := cert_data.get('applicant_id'):
            try:
                self.db_service._execute_query(
                    "DELETE FROM applicants WHERE id = %s",
                    (applicant_id,)
                )
            except Exception as e:
                logger.error(f"Failed to rollback applicant: {e}")

    def rollback_certification_body(self, cert_data: Dict[str, Any]) -> None:
        """Компенсация для save_certification_body"""
        if cert_body_id := cert_data.get('certification_body_id'):
            try:
                self.db_service._execute_query(
                    "DELETE FROM certification_bodies WHERE id = %s",
                    (cert_body_id,)
                )
            except Exception as e:
                logger.error(f"Failed to rollback certification body: {e}")

    def process_certificate(self, cert_data: Dict[str, Any]) -> bool:
        """Основной метод обработки сертификата"""
        try:
            logger.info("=== Starting certificate processing ===")
            logger.info(f"Initial cert_data keys: {cert_data.keys()}")
            
            # 1. Сохраняем производителя
            if not self.save_manufacturer(cert_data):
                logger.error("Failed to save manufacturer")
                return False
            
            logger.info(f"After manufacturer save, manufacturer_id: {cert_data.get('manufacturer_id')}")
            
            # 2. Сохраняе�� заявителя
            if not self.save_applicant(cert_data):
                logger.error("Failed to save applicant")
                return False
            
            # 3. Сохраняем орган сертификации
            if not self.save_certification_body(cert_data):
                logger.error("Failed to save certification body")
                return False
            
            # Проверяем наличие всех необходимых ID
            required_ids = ['manufacturer_id', 'applicant_id', 'certification_body_id']
            missing_ids = [id_field for id_field in required_ids if id_field not in cert_data]
            
            if missing_ids:
                logger.error(f"Missing required IDs: {missing_ids}")
                logger.error(f"Current cert_data keys: {cert_data.keys()}")
                return False
            
            # 4. Сохраняем сертификат
            if not self.save_certificate(cert_data):
                logger.error("Failed to save certificate")
                return False
            
            logger.info("=== Certificate processing completed successfully ===")
            return True
            
        except Exception as e:
            logger.error(f"Error in process_certificate: {e}")
            logger.error(f"Current cert_data state: {cert_data}")
            return False

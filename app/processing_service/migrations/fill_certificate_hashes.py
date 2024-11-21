from typing import Dict, Any
import hashlib
import json
import logging
from database import DatabaseConnection
from sagas.certificate_saga import CertificateSaga

logger = logging.getLogger(__name__)

def generate_certificate_hash(cert_data: Dict[str, Any]) -> str:
    """Генерация хеша сертификата"""
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
    
    hash_data = {
        field: str(cert_data.get(field, '')) 
        for field in hash_fields 
        if field in cert_data
    }
    
    hash_string = json.dumps(hash_data, sort_keys=True)
    return hashlib.sha256(hash_string.encode()).hexdigest()

def fill_certificate_hashes(db: DatabaseConnection):
    """Заполнение хешей для существующих сертификатов"""
    try:
        # Получаем все сертификаты без хешей
        certificates = db._execute_query("""
            SELECT 
                c.id as certificate_id,
                c.cert_number,
                c.product_name,
                c.product_group,
                c.cert_registration_date,
                c.cert_expiration_date,
                m.manufacturer_name,
                m.manufacturer_inn,
                a.applicant_name,
                a.applicant_inn,
                cb.certification_body_number
            FROM certificates c
            LEFT JOIN manufacturers m ON c.manufacturer_id = m.id
            LEFT JOIN applicants a ON c.applicant_id = a.id
            LEFT JOIN certification_bodies cb ON c.certification_body_id = cb.id
            WHERE NOT EXISTS (
                SELECT 1 FROM certificate_hashes ch 
                WHERE ch.certificate_id = c.id
            );
        """)

        if not certificates:
            logger.info("No certificates without hashes found")
            return

        logger.info(f"Found {len(certificates)} certificates without hashes")

        # Создаем хеши пакетами
        batch_size = 1000
        for i in range(0, len(certificates), batch_size):
            batch = certificates[i:i + batch_size]
            values = []
            
            for cert in batch:
                cert_data = {
                    'cert_number': cert[1],
                    'product_name': cert[2],
                    'product_group': cert[3],
                    'cert_registration_date': cert[4],
                    'cert_expiration_date': cert[5],
                    'manufacturer_name': cert[6],
                    'manufacturer_inn': cert[7],
                    'applicant_name': cert[8],
                    'applicant_inn': cert[9],
                    'certification_body_number': cert[10]
                }
                
                hash_value = generate_certificate_hash(cert_data)
                values.append((cert[0], hash_value))

            # Вставляем хеши пакетом
            db._execute_query("""
                INSERT INTO certificate_hashes (certificate_id, hash_value)
                VALUES %s
                ON CONFLICT (certificate_id) DO NOTHING;
            """, values)

            logger.info(f"Processed batch of {len(batch)} certificates")

        logger.info("Successfully filled certificate hashes")

    except Exception as e:
        logger.error(f"Error filling certificate hashes: {e}")
        raise

if __name__ == "__main__":
    from common.config import POSTGRES_DSN
    db = DatabaseConnection(POSTGRES_DSN)
    fill_certificate_hashes(db) 
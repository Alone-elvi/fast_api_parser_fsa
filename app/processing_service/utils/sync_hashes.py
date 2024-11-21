import sys
import os
import hashlib
import logging
import psycopg2
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_certificate_hash(cert_data: Dict[str, Any]) -> str:
    """Создает SHA-256 хеш из данных сертификата"""
    hash_fields = {
        'registration_number': cert_data.get('registration_number'),
        'product_name': cert_data.get('product_name'),
        'registration_date': str(cert_data.get('registration_date')),
        'expiration_date': str(cert_data.get('expiration_date')),
        'status': cert_data.get('status')
    }
    
    hash_string = '|'.join(str(value) for value in sorted(hash_fields.values()) if value)
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

def analyze_certificates():
    """Анализ сертификатов и их хешей"""
    db_params = {
        'dbname': 'fsa_parser_db',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'postgres',
        'port': '5432'
    }

    try:
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Общая статистика
        cur.execute("SELECT COUNT(*) FROM certificates")
        total_certificates = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM certificate_hashes")
        total_hashes = cur.fetchone()[0]
        
        # Проверяем дубликаты по registration_number
        cur.execute("""
            SELECT registration_number, COUNT(*)
            FROM certificates
            GROUP BY registration_number
            HAVING COUNT(*) > 1
        """)
        duplicate_certs = cur.fetchall()
        
        # Сертификаты без хешей
        cur.execute("""
            SELECT COUNT(*)
            FROM certificates c
            LEFT JOIN certificate_hashes ch ON c.id = ch.certificate_id
            WHERE ch.id IS NULL
        """)
        certs_without_hash = cur.fetchone()[0]
        
        # Хеши без сертификатов (orphaned)
        cur.execute("""
            SELECT COUNT(*)
            FROM certificate_hashes ch
            LEFT JOIN certificates c ON ch.certificate_id = c.id
            WHERE c.id IS NULL
        """)
        orphaned_hashes = cur.fetchone()[0]
        
        # Вывод статистики
        logger.info(f"""
        Certificate Analysis:
        ---------------------
        Total certificates: {total_certificates}
        Total hashes: {total_hashes}
        Certificates without hash: {certs_without_hash}
        Orphaned hashes: {orphaned_hashes}
        Duplicate certificates: {len(duplicate_certs)}
        
        Detailed duplicate analysis:
        """)
        
        if duplicate_certs:
            for reg_num, count in duplicate_certs:
                logger.info(f"Registration number {reg_num}: {count} duplicates")
                
                # Показываем детали дубликатов
                cur.execute("""
                    SELECT id, registration_date, updated_at
                    FROM certificates
                    WHERE registration_number = %s
                    ORDER BY updated_at DESC
                """, (reg_num,))
                details = cur.fetchall()
                for cert_id, reg_date, updated in details:
                    logger.info(f"  ID: {cert_id}, Registered: {reg_date}, Updated: {updated}")
        
        # Предлагаем решение
        if total_certificates > total_hashes:
            logger.info("""
            Recommended actions
            """)
        
    except Exception as e:
        logger.error(f"Error analyzing certificates: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    analyze_certificates()
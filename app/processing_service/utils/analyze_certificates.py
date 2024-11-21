import logging
import psycopg2
from typing import List, Tuple

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_certificates():
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
        
        # Проверяем количество записей по датам
        cur.execute("""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as count
            FROM certificates
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            LIMIT 10
        """)
        cert_by_date = cur.fetchall()
        
        # Проверяем количество хешей по датам
        cur.execute("""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as count
            FROM certificate_hashes
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            LIMIT 10
        """)
        hash_by_date = cur.fetchall()
        
        # Проверяем последние обновления
        cur.execute("""
            SELECT 
                DATE(updated_at) as date,
                COUNT(*) as count
            FROM certificates
            GROUP BY DATE(updated_at)
            ORDER BY date DESC
            LIMIT 10
        """)
        updates_by_date = cur.fetchall()
        
        # Проверяем статусы сертификатов
        cur.execute("""
            SELECT 
                status,
                COUNT(*) as count
            FROM certificates
            GROUP BY status
        """)
        status_counts = cur.fetchall()
        
        logger.info("""
        Certificate Analysis:
        --------------------
        Certificates by creation date:
        """)
        for date, count in cert_by_date:
            logger.info(f"  {date}: {count} certificates")
            
        logger.info("\nHashes by creation date:")
        for date, count in hash_by_date:
            logger.info(f"  {date}: {count} hashes")
            
        logger.info("\nCertificates by last update:")
        for date, count in updates_by_date:
            logger.info(f"  {date}: {count} updates")
            
        logger.info("\nCertificates by status:")
        for status, count in status_counts:
            logger.info(f"  {status}: {count}")
            
        # Проверяем несоответствия между сертификатами и хешами
        cur.execute("""
            SELECT c.id, c.registration_number, 
                   (SELECT COUNT(*) FROM certificate_hashes WHERE certificate_id = c.id) as hash_count
            FROM certificates c
            WHERE (SELECT COUNT(*) FROM certificate_hashes WHERE certificate_id = c.id) != 1
        """)
        mismatches = cur.fetchall()
        
        if mismatches:
            logger.info("\nCertificates with hash mismatches:")
            for cert_id, reg_num, hash_count in mismatches:
                logger.info(f"  Certificate ID {cert_id} ({reg_num}): {hash_count} hashes")
                
    except Exception as e:
        logger.error(f"Error analyzing certificates: {e}", exc_info=True)
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def analyze_duplicate_hashes():
    db_params = {
        'dbname': 'fsa_parser_db',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'postgres',
        'port': '5432'
    }

    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Проверяем детали дублирующих хешей
        cur.execute("""
            WITH duplicate_hashes AS (
                SELECT certificate_id, COUNT(*) as hash_count
                FROM certificate_hashes
                GROUP BY certificate_id
                HAVING COUNT(*) > 1
            )
            SELECT 
                c.registration_number,
                ch.hash_value,
                ch.created_at,
                ch.id as hash_id
            FROM duplicate_hashes dh
            JOIN certificates c ON c.id = dh.certificate_id
            JOIN certificate_hashes ch ON ch.certificate_id = c.id
            ORDER BY c.registration_number, ch.created_at
            LIMIT 10
        """)
        
        duplicates = cur.fetchall()
        
        logger.info("Sample of duplicate hashes:")
        current_reg = None
        for reg_num, hash_val, created_at, hash_id in duplicates:
            if reg_num != current_reg:
                logger.info(f"\nCertificate {reg_num}:")
                current_reg = reg_num
            logger.info(f"  Hash ID: {hash_id}, Value: {hash_val[:10]}..., Created: {created_at}")
            
        # Подсчет общего количества дубликатов
        cur.execute("""
            SELECT COUNT(*) 
            FROM (
                SELECT certificate_id
                FROM certificate_hashes
                GROUP BY certificate_id
                HAVING COUNT(*) > 1
            ) as duplicates
        """)
        total_duplicates = cur.fetchone()[0]
        
        logger.info(f"\nTotal certificates with duplicate hashes: {total_duplicates}")
        
        # SQL для исправления
        logger.info("""
        To fix this, run the following SQL:
        
        WITH duplicate_hashes AS (
            SELECT id,
                   ROW_NUMBER() OVER (PARTITION BY certificate_id ORDER BY created_at DESC) as rn
            FROM certificate_hashes
        )
        DELETE FROM certificate_hashes
        WHERE id IN (
            SELECT id 
            FROM duplicate_hashes 
            WHERE rn > 1
        );
        """)
        
    except Exception as e:
        logger.error(f"Error analyzing duplicate hashes: {e}", exc_info=True)
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    analyze_certificates()
    analyze_duplicate_hashes() 
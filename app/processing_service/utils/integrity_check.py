import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_data_integrity():
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
        
        # Проверяем сертификаты без хешей
        cur.execute("""
            SELECT c.id, c.registration_number
            FROM certificates c
            LEFT JOIN certificate_hashes ch ON c.id = ch.certificate_id
            WHERE ch.id IS NULL
        """)
        missing_hashes = cur.fetchall()
        
        # Проверяем хеши без сертификатов
        cur.execute("""
            SELECT ch.id, ch.certificate_id
            FROM certificate_hashes ch
            LEFT JOIN certificates c ON c.id = ch.certificate_id
            WHERE c.id IS NULL
        """)
        orphaned_hashes = cur.fetchall()
        
        # Проверяем несоответствия в данных
        cur.execute("""
            SELECT c.id, c.registration_number, 
                   c.status, c.expiration_date,
                   CASE 
                       WHEN c.status = 'Действует' AND c.expiration_date < CURRENT_DATE THEN 'Expired but Active'
                       WHEN c.status != 'Действует' AND c.expiration_date > CURRENT_DATE THEN 'Valid but Inactive'
                   END as inconsistency
            FROM certificates c
            WHERE (c.status = 'Действует' AND c.expiration_date < CURRENT_DATE)
               OR (c.status != 'Действует' AND c.expiration_date > CURRENT_DATE)
        """)
        inconsistencies = cur.fetchall()
        
        # Выводим результаты
        if missing_hashes:
            logger.warning(f"Found {len(missing_hashes)} certificates without hashes")
            for cert_id, reg_num in missing_hashes[:5]:
                logger.warning(f"  Missing hash for certificate {reg_num} (ID: {cert_id})")
                
        if orphaned_hashes:
            logger.warning(f"Found {len(orphaned_hashes)} orphaned hashes")
            for hash_id, cert_id in orphaned_hashes[:5]:
                logger.warning(f"  Orphaned hash ID {hash_id} for certificate ID {cert_id}")
                
        if inconsistencies:
            logger.warning(f"Found {len(inconsistencies)} data inconsistencies")
            for cert_id, reg_num, status, exp_date, issue in inconsistencies[:5]:
                logger.warning(f"  {reg_num}: {issue} (status: {status}, expires: {exp_date})")
                
        if not any([missing_hashes, orphaned_hashes, inconsistencies]):
            logger.info("All integrity checks passed")
            
    except Exception as e:
        logger.error(f"Error checking data integrity: {e}", exc_info=True)
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close() 
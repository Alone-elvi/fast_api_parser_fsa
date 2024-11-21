import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logger = logging.getLogger(__name__)

def fix_duplicate_hashes():
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
        
        # Удаляем дубликаты, оставляя самый последний хеш
        cur.execute("""
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
            )
            RETURNING id;
        """)
        
        deleted_count = cur.rowcount
        conn.commit()
        
        logger.info(f"Deleted {deleted_count} duplicate hashes")
        
    except Exception as e:
        logger.error(f"Error fixing duplicate hashes: {e}", exc_info=True)
        conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    fix_duplicate_hashes() 
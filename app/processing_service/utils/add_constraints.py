import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_database_constraints():
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
        
        # Добавляем уникальный индекс на certificate_id
        cur.execute("""
            ALTER TABLE certificate_hashes 
            ADD CONSTRAINT unique_certificate_hash 
            UNIQUE (certificate_id);
        """)
        
        # Добавляем foreign key constraint
        cur.execute("""
            ALTER TABLE certificate_hashes 
            ADD CONSTRAINT fk_certificate 
            FOREIGN KEY (certificate_id) 
            REFERENCES certificates(id) 
            ON DELETE CASCADE;
        """)
        
        conn.commit()
        logger.info("Database constraints added successfully")
        
    except Exception as e:
        logger.error(f"Error adding constraints: {e}", exc_info=True)
        conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close() 
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def run_query(query, params=None, fetch=True):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                results = cur.fetchall()
                return [dict(r) for r in results]
            else:
                conn.commit()
                return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_store_by_phone(phone_number):
    results = run_query(
        "select * from stores where phone_number = %s",
        (phone_number,)
    )
    return results[0] if results else None

def get_store_by_name(shop_name):
    results = run_query(
        "select * from stores where shop_name = %s",
        (shop_name,)
    )
    return results[0] if results else None
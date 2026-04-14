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
    # Normalize: strip +, spaces
    normalized = phone_number.strip().replace("+", "").replace(" ", "")
    results = run_query(
        '''
        select * from stores
        where replace(replace(phone_number, '+', ''), ' ', '') = %s
          and coalesce(is_active, true) = true
        ''',
        (normalized,)
    )
    return results[0] if results else None

def get_store_by_name(shop_name):
    results = run_query(
        '''
        select * from stores
        where shop_name = %s
          and coalesce(is_active, true) = true
        ''',
        (shop_name,)
    )
    return results[0] if results else None

def table_exists(table_name):
    """Check if a table exists in the database"""
    results = run_query('''
        select exists (
            select from information_schema.tables
            where table_name = %s
        ) as exists
    ''', (table_name,))
    return results[0]['exists'] if results else False

@app.post("/request-plan-change")
def request_plan_change(
    request: Request,
    requested_plan: str = Form(...),
    note: str = Form("")
):
    phone = request.cookies.get("phone")
    if not phone:
        return RedirectResponse(url="/login", status_code=302)

    store = get_store_by_phone_number(phone)
    if not store:
        return RedirectResponse(url="/login", status_code=302)

    run_query('''
        insert into plan_requests
        (store_id, current_plan, requested_plan, note)
        values (%s, %s, %s, %s)
    ''', (store['id'], store['plan'], requested_plan, note),
    fetch=False)

    return RedirectResponse(
        url="/dashboard?success=Plan+change+request+sent+to+admin",
        status_code=302
    )

@app.post("/admin/update-plan")
def update_plan(
    request: Request,
    store_id: int = Form(...),
    new_plan: str = Form(...),
    request_id: int = Form(None)
):
    phone = request.cookies.get("phone")
    if not phone or not is_admin(phone):
        return RedirectResponse(url="/login", status_code=302)

    run_query('''
        update stores set plan = %s where id = %s
    ''', (new_plan, store_id), fetch=False)

    if request_id:
        run_query('''
            update plan_requests set status = 'approved'
            where id = %s
        ''', (request_id,), fetch=False)

    return RedirectResponse(url="/admin", status_code=302)
import os
import httpx
import tempfile
from fastapi import FastAPI, Request, BackgroundTasks, Form, UploadFile, File
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from app.database import get_store_by_phone, run_query
from app.processor import process_file
from app.whatsapp import send_store_summary, send_whatsapp_message
from app.auth import send_otp, verify_otp, is_admin, get_store_by_phone_number
from app.analytics import (
    get_store_summary, get_top_products,
    get_low_stock, get_dead_stock, get_daily_trend
)
from datetime import date

load_dotenv()

app = FastAPI()

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ─────────────────────────────────────────
# HOME
# ─────────────────────────────────────────

@app.get("/")
def home():
    return RedirectResponse(url="/login", status_code=302)

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────

@app.get("/health")
def health():
    from datetime import datetime, timezone
    return {"status": "ok", "timestamp": datetime.now(timezone.utc)}

# ─────────────────────────────────────────
# AUTH — LOGIN
# ─────────────────────────────────────────

@app.get("/login")
def login_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"error": None}
    )

@app.post("/login")
def login_submit(request: Request, phone: str = Form(...)):
    phone = phone.strip().replace("+", "").replace(" ", "")

    store = get_store_by_phone_number(phone)
    admin = is_admin(phone)

    if not store and not admin:
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "This number is not registered. Please contact support."}
        )

    send_otp(phone)

    return templates.TemplateResponse(
        request=request,
        name="otp.html",
        context={"phone": phone, "error": None}
    )

# ─────────────────────────────────────────
# AUTH — VERIFY OTP
# ─────────────────────────────────────────

@app.post("/verify-otp")
def verify_otp_submit(
    request: Request,
    phone: str = Form(...),
    otp: str = Form(...)
):
    phone = phone.strip()
    otp   = otp.strip()

    if not verify_otp(phone, otp):
        return templates.TemplateResponse(
            request=request,
            name="otp.html",
            context={
                "phone": phone,
                "error": "Invalid or expired OTP. Please try again."
            }
        )

    if is_admin(phone):
        response = RedirectResponse(url="/admin", status_code=302)
    else:
        response = RedirectResponse(url="/dashboard", status_code=302)

    response.set_cookie(
        key="phone",
        value=phone,
        httponly=True,
        max_age=86400
    )
    return response

# ─────────────────────────────────────────
# LOGOUT
# ─────────────────────────────────────────

@app.get("/logout")
def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("phone")
    return response

# ─────────────────────────────────────────
# STORE DASHBOARD
# ─────────────────────────────────────────

@app.get("/dashboard")
def dashboard(request: Request):
    phone = request.cookies.get("phone")
    if not phone:
        return RedirectResponse(url="/login", status_code=302)

    # Admin should use /admin not /dashboard
    if is_admin(phone):
        return RedirectResponse(url="/admin", status_code=302)

    store = get_store_by_phone_number(phone)
    if not store:
        return RedirectResponse(url="/login", status_code=302)

    store_id     = store['id']
    summary      = get_store_summary(store_id, days=7)
    top_products = get_top_products(store_id, days=7, limit=5)
    low_stock    = get_low_stock(store_id)
    dead_stock   = get_dead_stock(store_id)
    daily_trend  = get_daily_trend(store_id, days=7)
    uploads      = run_query('''
        select * from uploads
        where store_id = %s
        order by uploaded_at desc
        limit 10
    ''', (store_id,))

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "store":        store,
            "summary":      summary,
            "top_products": top_products,
            "low_stock":    low_stock,
            "dead_stock":   dead_stock,
            "daily_trend":  daily_trend,
            "uploads":      uploads,
            "today":        date.today().isoformat() 
        }
    )

# ─────────────────────────────────────────
# ADMIN DASHBOARD
# ─────────────────────────────────────────

@app.get("/admin")
def admin_dashboard(request: Request):
    phone = request.cookies.get("phone")
    if not phone or not is_admin(phone):
        return RedirectResponse(url="/login", status_code=302)

    stores = run_query('''
        select
            s.*,
            max(u.uploaded_at)                      as last_upload,
            current_date - max(u.uploaded_at::date) as days_inactive,
            coalesce(sum(sr.gross_revenue), 0)       as revenue_7d
        from stores s
        left join uploads u on u.store_id = s.id
        left join sales_raw sr on sr.store_id = s.id
            and sr.sale_date >= current_date - 7
        group by s.id
        order by s.created_at desc
    ''')

    total_revenue = run_query('''
        select coalesce(sum(gross_revenue), 0) as total
        from sales_raw
        where sale_date >= current_date - 7
    ''')

    total_uploads = run_query('''
        select count(*) as total from uploads
    ''')

    inactive_stores = sum(
        1 for s in stores
        if s['days_inactive'] and s['days_inactive'] > 3
    )

    recent_uploads = run_query('''
        select u.*, s.shop_name
        from uploads u
        join stores s on s.id = u.store_id
        order by u.uploaded_at desc
        limit 20
    ''')

    return templates.TemplateResponse(
        request=request,
        name="admin.html",
        context={
            "stores":          stores,
            "total_revenue":   total_revenue[0]['total'] if total_revenue else 0,
            "total_uploads":   total_uploads[0]['total'] if total_uploads else 0,
            "inactive_stores": inactive_stores,
            "recent_uploads":  recent_uploads,
            "add_error":       None,
            "add_success":     None
        }
    )

@app.post("/admin/add-store")
def add_store(
    request: Request,
    shop_name:    str = Form(...),
    owner_name:   str = Form(...),
    phone_number: str = Form(...),
    store_type:   str = Form(...),
    address:      str = Form(...),
    plan:         str = Form(...)
):
    phone = request.cookies.get("phone")
    if not phone or not is_admin(phone):
        return RedirectResponse(url="/login", status_code=302)

    try:
        run_query('''
            insert into stores
            (shop_name, owner_name, phone_number, address, store_type, plan)
            values (%s, %s, %s, %s, %s, %s)
        ''', (
            shop_name.lower().strip(),
            owner_name.strip(),
            phone_number.strip(),
            address.strip(),
            store_type.strip(),
            plan
        ), fetch=False)
        success = f"Store '{shop_name}' added successfully!"
        error   = None
    except Exception as e:
        success = None
        error   = f"Error adding store: {str(e)}"

    stores = run_query('''
        select s.*,
            max(u.uploaded_at) as last_upload,
            current_date - max(u.uploaded_at::date) as days_inactive,
            coalesce(sum(sr.gross_revenue), 0) as revenue_7d
        from stores s
        left join uploads u on u.store_id = s.id
        left join sales_raw sr on sr.store_id = s.id
            and sr.sale_date >= current_date - 7
        group by s.id
        order by s.created_at desc
    ''')

    total_revenue = run_query('''
        select coalesce(sum(gross_revenue), 0) as total
        from sales_raw where sale_date >= current_date - 7
    ''')

    total_uploads = run_query('''
        select count(*) as total from uploads
    ''')

    inactive_stores = sum(
        1 for s in stores
        if s['days_inactive'] and s['days_inactive'] > 3
    )

    recent_uploads = run_query('''
        select u.*, s.shop_name
        from uploads u
        join stores s on s.id = u.store_id
        order by u.uploaded_at desc
        limit 20
    ''')

    return templates.TemplateResponse(
        request=request,
        name="admin.html",
        context={
            "stores":          stores,
            "total_revenue":   total_revenue[0]['total'] if total_revenue else 0,
            "total_uploads":   total_uploads[0]['total'] if total_uploads else 0,
            "inactive_stores": inactive_stores,
            "recent_uploads":  recent_uploads,
            "add_error":       error,
            "add_success":     success
        }
    )

@app.get("/admin/store/{store_id}")
def admin_store_detail(request: Request, store_id: int):
    phone = request.cookies.get("phone")
    if not phone or not is_admin(phone):
        return RedirectResponse(url="/login", status_code=302)

    store = run_query("select * from stores where id = %s", (store_id,))
    if not store:
        return RedirectResponse(url="/admin", status_code=302)

    store        = store[0]
    summary      = get_store_summary(store_id, days=7)
    top_products = get_top_products(store_id, days=7, limit=5)
    low_stock    = get_low_stock(store_id)
    dead_stock   = get_dead_stock(store_id)
    daily_trend  = get_daily_trend(store_id, days=7)
    uploads      = run_query('''
        select * from uploads
        where store_id = %s
        order by uploaded_at desc
        limit 10
    ''', (store_id,))

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "store":        store,
            "summary":      summary,
            "top_products": top_products,
            "low_stock":    low_stock,
            "dead_stock":   dead_stock,
            "daily_trend":  daily_trend,
            "uploads":      uploads
        }
    )

# ─────────────────────────────────────────
# FILE UPLOAD FROM DASHBOARD
# ─────────────────────────────────────────

@app.post("/upload-file")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    store_id: int = None
):
    phone = request.cookies.get("phone")
    if not phone:
        return RedirectResponse(url="/login", status_code=302)

    # If store_id provided and user is admin — use that store
    # Otherwise use the logged in user's store
    if store_id and is_admin(phone):
        store = run_query(
            "select * from stores where id = %s", (store_id,)
        )
        store = store[0] if store else None
    else:
        store = get_store_by_phone_number(phone)

    if not store:
        return RedirectResponse(url="/login", status_code=302)

    try:
        suffix = os.path.splitext(file.filename)[1] or '.xlsx'
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix
        ) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name

        result = process_file(tmp_path, store['id'])

        run_query('''
            insert into uploads
            (store_id, file_name, rows_processed, rows_failed, status)
            values (%s, %s, %s, %s, %s)
        ''', (
            store['id'],
            file.filename,
            result['rows_processed'],
            result['rows_failed'],
            result['status']
        ), fetch=False)

        os.unlink(tmp_path)

        redirect_url = f"/admin/store/{store['id']}?success=Processed+{result['rows_processed']}+rows" \
            if is_admin(phone) else \
            f"/dashboard?success=Processed+{result['rows_processed']}+rows+successfully"

        return RedirectResponse(url=redirect_url, status_code=302)

    except Exception as e:
        print(f"Upload error: {e}")
        redirect_url = f"/admin/store/{store['id']}?error=File+processing+failed" \
            if is_admin(phone) else \
            "/dashboard?error=File+processing+failed.+Check+column+names."
        return RedirectResponse(url=redirect_url, status_code=302)


@app.post("/add-sale")
def add_sale(
    request: Request,
    store_id:       int = None,
    sale_date:      str = Form(...),
    product_name:   str = Form(...),
    category:       str = Form(""),
    quantity_sold:  float = Form(...),
    selling_price:  float = Form(...),
    purchase_price: float = Form(0),
    opening_stock:  float = Form(None),
    closing_stock:  float = Form(None)
):
    phone = request.cookies.get("phone")
    if not phone:
        return RedirectResponse(url="/login", status_code=302)

    # If store_id provided and user is admin — use that store
    if store_id and is_admin(phone):
        store = run_query(
            "select * from stores where id = %s", (store_id,)
        )
        store = store[0] if store else None
    else:
        store = get_store_by_phone_number(phone)

    if not store:
        return RedirectResponse(url="/login", status_code=302)

    try:
        run_query('''
            insert into sales_raw
            (store_id, sale_date, product_name, category,
             quantity_sold, selling_price, purchase_price,
             opening_stock, closing_stock)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            store['id'],
            sale_date,
            product_name.lower().strip(),
            category.lower().strip() or None,
            quantity_sold,
            selling_price,
            purchase_price,
            opening_stock,
            closing_stock
        ), fetch=False)

        redirect_url = f"/admin/store/{store['id']}?success=Sale+added+successfully" \
            if is_admin(phone) else \
            "/dashboard?success=Sale+added+successfully"

        return RedirectResponse(url=redirect_url, status_code=302)

    except Exception as e:
        print(f"Manual entry error: {e}")
        redirect_url = f"/admin/store/{store['id']}?error=Failed+to+add+sale" \
            if is_admin(phone) else \
            "/dashboard?error=Failed+to+add+sale.+Please+try+again."
        return RedirectResponse(url=redirect_url, status_code=302)

# ─────────────────────────────────────────
# WHATSAPP WEBHOOK
# ─────────────────────────────────────────

@app.post("/webhook")
async def receive_message(
    request: Request,
    background_tasks: BackgroundTasks,
    From: str = Form(None),
    Body: str = Form(None),
    MediaUrl0: str = Form(None),
    MediaContentType0: str = Form(None),
    NumMedia: str = Form(None)
):
    phone = From.replace("whatsapp:+", "") if From else None

    if not phone:
        return PlainTextResponse("ok")

    store = get_store_by_phone(phone)
    if not store:
        send_whatsapp_message(phone,
            "Sorry, your number is not registered. "
            "Please contact support.")
        return PlainTextResponse("ok")

    if NumMedia and int(NumMedia) > 0 and MediaUrl0:
        background_tasks.add_task(
            handle_file_upload,
            MediaUrl0, MediaContentType0, store
        )
        send_whatsapp_message(phone,
            "✅ File received! Processing your data now. "
            "You'll get your summary shortly.")

    elif Body:
        text = Body.lower().strip()
        if text in ['summary', 'report', 'stats']:
            background_tasks.add_task(
                send_store_summary,
                store['id'], store['shop_name'], phone
            )
        else:
            background_tasks.add_task(
                handle_ai_question, Body, store
            )

    return PlainTextResponse("ok")

# ─────────────────────────────────────────
# FILE UPLOAD HANDLER
# ─────────────────────────────────────────

async def handle_file_upload(media_url, content_type, store):
    phone    = store['phone_number']
    store_id = store['id']

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                media_url,
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                follow_redirects=True,
                headers={"Accept": "*/*"}
            )

        ext_map = {
            'application/vnd.openxmlformats-officedocument'
            '.spreadsheetml.sheet': '.xlsx',
            'application/vnd.ms-excel': '.xls',
            'text/csv':   '.csv',
            'text/plain': '.csv'
        }
        suffix = ext_map.get(content_type, '.xlsx')

        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix
        ) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        result = process_file(tmp_path, store_id)

        run_query('''
            insert into uploads
            (store_id, file_name, rows_processed, rows_failed, status)
            values (%s, %s, %s, %s, %s)
        ''', (
            store_id, f"upload{suffix}",
            result['rows_processed'],
            result['rows_failed'],
            result['status']
        ), fetch=False)

        send_whatsapp_message(phone,
            f"✅ Processed *{result['rows_processed']}* rows successfully!")
        send_store_summary(store_id, store['shop_name'], phone)
        os.unlink(tmp_path)

    except Exception as e:
        print(f"File processing error: {e}")
        send_whatsapp_message(phone,
            "❌ Something went wrong processing your file.\n"
            "Please make sure it has these columns:\n"
            "product_name, quantity_sold, selling_price, "
            "purchase_price, sale_date")

# ─────────────────────────────────────────
# AI Q&A HANDLER
# ─────────────────────────────────────────

async def handle_ai_question(question, store):
    try:
        from app.ai import ask_ai
        answer = ask_ai(
            question=question,
            store_id=store['id'],
            shop_name=store['shop_name']
        )
        send_whatsapp_message(store['phone_number'], answer)
    except Exception as e:
        print(f"AI error: {e}")
        send_whatsapp_message(store['phone_number'],
            "Sorry, I couldn't process your question right now. "
            "Try again in a moment.")
import os
import httpx
import tempfile
from fastapi import FastAPI, Request, BackgroundTasks, Form, UploadFile, File
from fastapi.responses import PlainTextResponse, RedirectResponse, FileResponse
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
from datetime import date, timedelta

load_dotenv()

app = FastAPI()

WA_TOKEN     = os.getenv("WA_TOKEN")
WA_PHONE_ID  = os.getenv("WA_PHONE_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xls", ".csv"}

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

@app.get("/demo/sample-file")
def download_sample_file():
    sample_path = "test_sales.csv"
    if not os.path.exists(sample_path):
        return PlainTextResponse("Sample file not found", status_code=404)
    return FileResponse(
        path=sample_path,
        media_type="text/csv",
        filename="storeiq-demo-sales.csv"
    )

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
            "today":        date.today().isoformat(),
            "is_admin_view": False
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
            "uploads":      uploads,
            "today":        date.today().isoformat(),
            "is_admin_view": True
        }
    )

def load_demo_sales_data(store_id: int):
    """Reset and seed 7 days of realistic demo sales."""
    today = date.today()
    sample_rows = [
        ("amul milk", "dairy", 42, 30, 26, 140, 98),
        ("parle g", "biscuits", 65, 10, 8, 220, 155),
        ("tata salt", "staples", 24, 25, 20, 90, 66),
        ("maggi", "snacks", 31, 15, 12, 120, 89),
        ("surf excel", "home care", 12, 95, 80, 50, 38),
    ]

    # Keep seeding idempotent by replacing recent sample window.
    run_query(
        '''
        delete from sales_raw
        where store_id = %s
          and sale_date >= current_date - 7
        ''',
        (store_id,),
        fetch=False
    )

    for day_idx in range(7):
        sale_day = today - timedelta(days=day_idx)
        for product_name, category, qty, sp, pp, opening, closing in sample_rows:
            # Vary quantity slightly by day for realistic trends.
            day_qty = max(qty - day_idx, 1)
            day_closing = max(closing - day_idx, 0)
            run_query(
                '''
                insert into sales_raw
                (store_id, sale_date, product_name, category,
                 quantity_sold, selling_price, purchase_price,
                 opening_stock, closing_stock)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (
                    store_id, sale_day, product_name, category,
                    day_qty, sp, pp, opening, day_closing
                ),
                fetch=False
            )


@app.post("/admin/store/{store_id}/load-demo-data")
def load_demo_data(request: Request, store_id: int):
    phone = request.cookies.get("phone")
    if not phone or not is_admin(phone):
        return RedirectResponse(url="/login", status_code=302)

    store = run_query("select id from stores where id = %s", (store_id,))
    if not store:
        return RedirectResponse(
            url=f"/admin/store/{store_id}?error=Store+not+found",
            status_code=302
        )

    try:
        load_demo_sales_data(store_id)
        return RedirectResponse(
            url=f"/admin/store/{store_id}?success=Demo+sales+data+loaded+for+last+7+days",
            status_code=302
        )
    except Exception as e:
        print(f"Demo data load error: {e}")
        return RedirectResponse(
            url=f"/admin/store/{store_id}?error=Failed+to+load+demo+data",
            status_code=302
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
        suffix = (os.path.splitext(file.filename or "")[1] or "").lower()
        if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
            redirect_url = (
                f"/admin/store/{store['id']}?error=Unsupported+file+type.+Use+.xlsx%2C+.xls%2C+or+.csv"
                if is_admin(phone) else
                "/dashboard?error=Unsupported+file+type.+Use+.xlsx%2C+.xls%2C+or+.csv"
            )
            return RedirectResponse(url=redirect_url, status_code=302)

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

        skipped_rows = max(result.get("rows_received", 0) - result['rows_processed'], 0)
        success_msg = (
            f"Processed {result['rows_processed']} rows successfully"
            if skipped_rows == 0 else
            f"Processed {result['rows_processed']} rows. Skipped {skipped_rows} invalid rows."
        )
        redirect_url = f"/admin/store/{store['id']}?success={success_msg.replace(' ', '+')}" \
            if is_admin(phone) else \
            f"/dashboard?success={success_msg.replace(' ', '+')}"

        return RedirectResponse(url=redirect_url, status_code=302)

    except ValueError as e:
        print(f"Upload validation error: {e}")
        error_msg = str(e).replace(" ", "+")
        redirect_url = f"/admin/store/{store['id']}?error={error_msg}" \
            if is_admin(phone) else \
            f"/dashboard?error={error_msg}"
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
# META WEBHOOK VERIFICATION
# ─────────────────────────────────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    mode      = request.query_params.get("hub.mode")
    token     = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    print(f"Webhook verify: mode={mode} token={token}")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("Webhook verified!")
        return PlainTextResponse(content=challenge)

    return PlainTextResponse(content="Forbidden", status_code=403)

# ─────────────────────────────────────────
# RECEIVE WHATSAPP MESSAGES FROM META
# ─────────────────────────────────────────
@app.post("/webhook")
async def receive_message(
    request: Request,
    background_tasks: BackgroundTasks
):
    body = await request.json()
    print(f"Webhook received: {body}")

    try:
        entry = body['entry'][0]['changes'][0]['value']

        # Ignore status updates
        if 'messages' not in entry:
            return {"status": "ignored"}

        message = entry['messages'][0]
        phone = message['from']
        msg_type = message['type']

        # Look up store by phone number
        store = get_store_by_phone(phone)
        if not store:
            send_whatsapp_message(
                phone,
                "Sorry, your number is not registered. "
                "Please contact support."
            )
            return {"status": "unregistered"}

        # Handle document/file message
        if msg_type == "document":
            file_id = message['document']['id']
            file_name = message['document'].get('filename', 'upload.xlsx')
            background_tasks.add_task(
                handle_file_upload,
                file_id, file_name, store
            )
            send_whatsapp_message(
                phone,
                "✅ File received! Processing your data now. "
                "You'll get your summary shortly."
            )

        # Handle text message
        elif msg_type == "text":
            text = message['text']['body'].lower().strip()
            if text in ["summary", "report", "stats"]:
                background_tasks.add_task(
                    send_store_summary,
                    store['id'],
                    store['shop_name'],
                    phone
                )
            else:
                background_tasks.add_task(
                    handle_ai_question,
                    message['text']['body'],
                    store
                )
        else:
            send_whatsapp_message(
                phone,
                "I can read text or spreadsheet documents only right now. "
                "Please send a .xlsx, .xls, or .csv file."
            )

        return {"status": "ok"}

    except Exception as e:
        print(f"Webhook error: {e}")
        return {"status": "error", "detail": str(e)}

# ─────────────────────────────────────────
# FILE UPLOAD FROM META WHATSAPP
# ─────────────────────────────────────────

async def handle_file_upload(file_id, file_name, store):
    phone    = store['phone_number']
    store_id = store['id']

    try:
        async with httpx.AsyncClient() as client:
            # Step 1 — Get file URL from Meta
            meta_response = await client.get(
                f"https://graph.facebook.com/v18.0/{file_id}",
                headers={"Authorization": f"Bearer {WA_TOKEN}"}
            )
            meta_response.raise_for_status()
            file_url = meta_response.json()['url']

            # Step 2 — Download the file
            file_response = await client.get(
                file_url,
                headers={"Authorization": f"Bearer {WA_TOKEN}"}
            )
            file_response.raise_for_status()

        # Step 3 — Determine extension
        suffix = (os.path.splitext(file_name)[1] or '.xlsx').lower()
        if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
            raise ValueError("Unsupported file type. Use .xlsx, .xls, or .csv")

        # Step 4 — Save to temp file
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix
        ) as tmp:
            tmp.write(file_response.content)
            tmp_path = tmp.name

        # Step 5 — Process the file
        result = process_file(tmp_path, store_id)

        # Step 6 — Log the upload
        run_query('''
            insert into uploads
            (store_id, file_name, rows_processed, rows_failed, status)
            values (%s, %s, %s, %s, %s)
        ''', (
            store_id, file_name,
            result['rows_processed'],
            result['rows_failed'],
            result['status']
        ), fetch=False)

        # Step 7 — Send confirmation + summary
        skipped_rows = max(result.get("rows_received", 0) - result['rows_processed'], 0)
        if skipped_rows > 0:
            send_whatsapp_message(
                phone,
                "✅ Processed "
                f"*{result['rows_processed']}* rows. "
                f"Skipped *{skipped_rows}* invalid rows."
            )
        else:
            send_whatsapp_message(
                phone,
                f"✅ Processed *{result['rows_processed']}* rows successfully!"
            )
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
import os
import httpx
import tempfile
from fastapi import FastAPI, Request, BackgroundTasks, Form
from fastapi.responses import PlainTextResponse
from dotenv import load_dotenv
from app.database import get_store_by_phone, run_query
from app.processor import process_file
from app.whatsapp import send_store_summary, send_whatsapp_message

load_dotenv()

app = FastAPI()

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────

@app.get("/health")
def health():
    from datetime import datetime, timezone
    return {"status": "ok", "timestamp": datetime.now(timezone.utc)}

@app.get("/")
def home():
    return {
        "product": "StoreIQ",
        "status": "running",
        "message": "WhatsApp Analytics for Local Stores"
    }

# ─────────────────────────────────────────
# RECEIVE WHATSAPP MESSAGES FROM TWILIO
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
    # Extract phone number — Twilio sends "whatsapp:+919876543210"
    phone = From.replace("whatsapp:+", "") if From else None

    if not phone:
        return PlainTextResponse("ok")

    # Look up store by phone number
    store = get_store_by_phone(phone)
    if not store:
        send_whatsapp_message(phone,
            "Sorry, your number is not registered. "
            "Please contact support.")
        return PlainTextResponse("ok")

    # Handle file/document message
    if NumMedia and int(NumMedia) > 0 and MediaUrl0:
        background_tasks.add_task(
            handle_file_upload,
            MediaUrl0,
            MediaContentType0,
            store
        )
        send_whatsapp_message(phone,
            "✅ File received! Processing your data now. "
            "You'll get your summary shortly.")

    # Handle text message
    elif Body:
        text = Body.lower().strip()
        if text in ['summary', 'report', 'stats']:
            background_tasks.add_task(
                send_store_summary,
                store['id'],
                store['shop_name'],
                phone
            )
        else:
            # Send any other question to AI
            background_tasks.add_task(
                handle_ai_question,
                Body,
                store
            )

    return PlainTextResponse("ok")

# ─────────────────────────────────────────
# HANDLE FILE UPLOAD IN BACKGROUND
# ─────────────────────────────────────────

async def handle_file_upload(media_url, content_type, store):
    phone = store['phone_number']
    store_id = store['id']

    try:
        # Step 1 — Download file from Twilio with basic auth
        async with httpx.AsyncClient() as client:
            response = await client.get(
                media_url,
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                follow_redirects=True,
                headers={"Accept": "*/*"}
            )

        print(f"Download status: {response.status_code}")
        print(f"Content type: {response.headers.get('content-type')}")
        print(f"Content length: {len(response.content)} bytes")

        # Step 2 — Determine file extension
        ext_map = {
            'application/vnd.openxmlformats-officedocument'
            '.spreadsheetml.sheet': '.xlsx',
            'application/vnd.ms-excel': '.xls',
            'text/csv': '.csv',
            'text/plain': '.csv'
        }
        suffix = ext_map.get(content_type, '.xlsx')

        # Step 3 — Save to temp file
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix
        ) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        print(f"Saved to: {tmp_path}")

        # Step 4 — Process the file
        result = process_file(tmp_path, store_id)

        # Step 5 — Log the upload
        run_query('''
            insert into uploads
            (store_id, file_name, rows_processed, rows_failed, status)
            values (%s, %s, %s, %s, %s)
        ''', (
            store_id,
            f"upload{suffix}",
            result['rows_processed'],
            result['rows_failed'],
            result['status']
        ), fetch=False)

        # Step 6 — Send confirmation + summary
        send_whatsapp_message(phone,
            f"✅ Processed *{result['rows_processed']}* rows successfully!")
        send_store_summary(store_id, store['shop_name'], phone)

        # Step 7 — Cleanup
        os.unlink(tmp_path)

    except Exception as e:
        print(f"File processing error: {e}")
        send_whatsapp_message(phone,
            "❌ Something went wrong processing your file.\n"
            "Please make sure it has these columns:\n"
            "product_name, quantity_sold, selling_price, "
            "purchase_price, sale_date")

# ─────────────────────────────────────────
# HANDLE AI QUESTION IN BACKGROUND
# ─────────────────────────────────────────

async def handle_ai_question(question, store):
    """Send question to AI and reply on WhatsApp"""
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
import os

import httpx
from dotenv import load_dotenv

from app.analytics import build_summary_bundle

load_dotenv()

WA_TOKEN = os.getenv("WA_TOKEN")
WA_PHONE_ID = os.getenv("WA_PHONE_ID")


def send_whatsapp_message(to_number, message):
    """Send a WhatsApp message via Meta API."""
    url = f"https://graph.facebook.com/v18.0/{WA_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {"body": message},
    }

    response = httpx.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"Message sent to {to_number}")
        return True

    print(f"Failed to send: {response.text}")
    return False


def format_summary_message(shop_name, summary_bundle):
    """Format analytics summary into an owner-friendly WhatsApp message."""
    return f"""🏪 *{shop_name.replace('_', ' ').title()}* — {summary_bundle['period_label']} Summary

💰 *Revenue:* ₹{summary_bundle['total_revenue']}
📈 *Profit:* ₹{summary_bundle['total_profit']} ({summary_bundle['margin_pct']}% margin)
📦 *Units Sold:* {summary_bundle['total_units']}

🔥 *Top Products:*
{summary_bundle['top_products'] or 'No standout products yet'}

⚠️ *Low Stock:* {summary_bundle['low_stock']}
😴 *Dead Stock:* {summary_bundle['dead_stock']}

_Powered by StoreIQ_"""


def send_store_summary(store_id, shop_name, phone_number, period_key="weekly"):
    """Build a summary for the requested period and send it to the owner."""
    summary_bundle = build_summary_bundle(store_id, period_key=period_key)
    message = format_summary_message(shop_name, summary_bundle)
    return send_whatsapp_message(phone_number, message)

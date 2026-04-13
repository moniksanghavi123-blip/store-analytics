import os
import httpx
from dotenv import load_dotenv
from app.analytics import build_summary

load_dotenv()

WA_TOKEN    = os.getenv("WA_TOKEN")
WA_PHONE_ID = os.getenv("WA_PHONE_ID")

def send_whatsapp_message(to_number, message):
    """Send a WhatsApp message via Meta API"""
    url = f"https://graph.facebook.com/v18.0/{WA_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {"body": message}
    }

    response = httpx.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        print(f"Message sent to {to_number}")
        return True
    else:
        print(f"Failed to send: {response.text}")
        return False

def format_summary_message(shop_name, summary):
    """Format analytics summary into WhatsApp message"""
    message = f"""🏪 *{shop_name.replace('_', ' ').title()}* — Weekly Summary

💰 *Revenue:* ₹{summary['total_revenue']}
📈 *Profit:* ₹{summary['total_profit']} ({summary['margin_pct']}% margin)
📦 *Units Sold:* {summary['total_units']}

🔥 *Top Products:*
{summary['top_products']}

⚠️ *Low Stock:* {summary['low_stock']}
😴 *Dead Stock:* {summary['dead_stock']}

_Powered by StoreIQ_ ✨"""
    return message

def send_store_summary(store_id, shop_name, phone_number):
    """Build summary and send to store owner"""
    summary = build_summary(store_id)
    message = format_summary_message(shop_name, summary)
    return send_whatsapp_message(phone_number, message)
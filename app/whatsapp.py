import os
from twilio.rest import Client
from dotenv import load_dotenv
from app.analytics import build_summary

load_dotenv()

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

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

def send_whatsapp_message(to_number, message):
    """Send a WhatsApp message via Twilio"""
    try:
        msg = client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            to=f"whatsapp:+{to_number}",
            body=message
        )
        print(f"Message sent to {to_number} — SID: {msg.sid}")
        return True
    except Exception as e:
        print(f"Failed to send message: {e}")
        return False

def send_store_summary(store_id, shop_name, phone_number):
    """Build summary and send to store owner"""
    summary = build_summary(store_id)
    message = format_summary_message(shop_name, summary)
    return send_whatsapp_message(phone_number, message)
import random
import string
import os
from datetime import datetime, timedelta
from app.database import run_query
from app.whatsapp import send_whatsapp_message
from dotenv import load_dotenv

load_dotenv()

ADMIN_PHONE = os.getenv("ADMIN_PHONE")

def generate_otp():
    """Generate 6 digit OTP"""
    return ''.join(random.choices(string.digits, k=6))

def save_otp(phone_number, otp):
    """Save OTP to database with 10 minute expiry"""
    # First create the OTP table if it doesn't exist
    run_query('''
        create table if not exists otp_codes (
            id          serial primary key,
            phone       text not null,
            otp         text not null,
            expires_at  timestamp not null,
            used        boolean default false,
            created_at  timestamp default now()
        )
    ''', fetch=False)

    # Delete any existing OTPs for this phone
    run_query('''
        delete from otp_codes where phone = %s
    ''', (phone_number,), fetch=False)

    # Save new OTP
    expires_at = datetime.now() + timedelta(minutes=10)
    run_query('''
        insert into otp_codes (phone, otp, expires_at)
        values (%s, %s, %s)
    ''', (phone_number, otp, expires_at), fetch=False)

def verify_otp(phone_number, otp):
    """Verify OTP is valid and not expired"""
    results = run_query('''
        select * from otp_codes
        where phone = %s
          and otp = %s
          and used = false
          and expires_at > now()
        order by created_at desc
        limit 1
    ''', (phone_number, otp))

    if not results:
        return False

    # Mark OTP as used
    run_query('''
        update otp_codes set used = true
        where phone = %s and otp = %s
    ''', (phone_number, otp), fetch=False)

    return True

def send_otp(phone_number):
    """Generate and send OTP via WhatsApp"""
    otp = generate_otp()
    save_otp(phone_number, otp)
    message = f"""🔐 *StoreIQ Login Code*

Your OTP is: *{otp}*

Valid for 10 minutes.
Do not share this with anyone."""
    send_whatsapp_message(phone_number, message)
    return True

def is_admin(phone_number):
    """Check if phone number is admin"""
    return phone_number == ADMIN_PHONE

def get_store_by_phone_number(phone_number):
    """Get store details by phone"""
    normalized = phone_number.strip().replace("+", "").replace(" ", "")
    results = run_query('''
        select * from stores
        where replace(replace(phone_number, '+', ''), ' ', '') = %s
          and coalesce(is_active, true) = true
    ''', (normalized,))
    return results[0] if results else None

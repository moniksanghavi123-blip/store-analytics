# StoreIQ — Business Analytics Platform

## Overview
StoreIQ is a business analytics and inventory management platform for local retail stores. Store owners upload sales data (CSV/Excel) and receive insights on revenue trends, profit margins, top products, and stock alerts via a web dashboard and WhatsApp.

## Architecture
- **Framework:** FastAPI (Python)
- **Database:** PostgreSQL (Replit built-in, via psycopg2)
- **Templates:** Jinja2 (server-side HTML rendering)
- **Auth:** OTP-based via WhatsApp (Twilio/Meta WhatsApp API)
- **AI:** Groq (Llama-3.3-70b) for conversational store analytics
- **Data Processing:** Pandas + Openpyxl for CSV/Excel ingestion

## Project Layout
```
app/
  main.py       - FastAPI routes and app startup
  database.py   - PostgreSQL connection + query helpers
  analytics.py  - Revenue, profit, stock analytics queries
  auth.py       - OTP generation, verification, admin check
  ai.py         - Groq AI assistant
  processor.py  - CSV/Excel file processing & normalization
  whatsapp.py   - WhatsApp messaging integration
static/
  style.css     - Frontend styles
templates/
  login.html    - Login page
  otp.html      - OTP verification
  dashboard.html - Store analytics dashboard
  admin.html    - Admin panel
```

## Database Schema
- `stores` — Store accounts (multi-tenant)
- `sales_raw` — Raw sales records (with generated revenue/profit columns)
- `uploads` — Upload history per store
- `otp_codes` — OTP records for authentication
- `store_column_mappings` — Custom CSV column mapping per store
- `plan_requests` — Store plan upgrade requests

## Plans
- **Starter** — Basic analytics only
- **Growth** — Charts + CSV column mapping
- **Pro** — Charts + CSV mapping + AI assistant

## Environment Variables
- `DATABASE_URL` — PostgreSQL connection string (auto-set by Replit)
- `ADMIN_PHONE` — Phone number of the admin user
- `VERIFY_TOKEN` — WhatsApp webhook verification token
- `WA_TOKEN` — Meta WhatsApp API token
- `WA_PHONE_ID` — Meta WhatsApp Phone ID
- `GROQ_API_KEY` — Groq API key for AI features

## Running Locally
```
uvicorn app.main:app --host 0.0.0.0 --port 5000
```

## Deployment
Configured for autoscale deployment on Replit.
Run command: `uvicorn app.main:app --host 0.0.0.0 --port 5000`

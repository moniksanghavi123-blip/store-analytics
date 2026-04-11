import os
from groq import Groq
from dotenv import load_dotenv
from app.analytics import (
    get_store_summary,
    get_top_products,
    get_low_stock,
    get_dead_stock,
    get_daily_trend,
    get_category_breakdown
)

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def build_store_context(store_id):
    """Pull all relevant store data to give AI context"""
    summary = get_store_summary(store_id, days=30)
    top_products = get_top_products(store_id, days=30, limit=5)
    low_stock = get_low_stock(store_id)
    dead_stock = get_dead_stock(store_id)
    daily_trend = get_daily_trend(store_id, days=7)
    categories = get_category_breakdown(store_id, days=30)

    context = f"""
STORE DATA — LAST 30 DAYS:

OVERALL SUMMARY:
- Total Revenue: ₹{summary.get('total_revenue', 0)}
- Total Profit: ₹{summary.get('total_profit', 0)}
- Profit Margin: {summary.get('margin_pct', 0)}%
- Total Units Sold: {summary.get('total_units', 0)}

TOP PRODUCTS:
{chr(10).join([f"- {p['product_name'].title()}: ₹{p['total_revenue']} revenue, {int(p['total_units'])} units, ₹{p['total_profit']} profit" for p in top_products])}

LOW STOCK ITEMS:
{chr(10).join([f"- {p['product_name'].title()}: {p['closing_stock']} units left" for p in low_stock if p['closing_stock'] and p['closing_stock'] < 5]) or 'None'}

DEAD STOCK (not sold in 14+ days):
{chr(10).join([f"- {p['product_name'].title()}: last sold {p['last_sold']}, {p['days_since_sold']} days ago" for p in dead_stock]) or 'None'}

DAILY REVENUE TREND (last 7 days):
{chr(10).join([f"- {r['sale_date']}: ₹{r['revenue']} revenue, ₹{r['profit']} profit" for r in daily_trend]) or 'No data'}

CATEGORY BREAKDOWN:
{chr(10).join([f"- {c['category'].title()}: ₹{c['revenue']} revenue, {int(c['units'])} units" for c in categories]) or 'No data'}
"""
    return context

def ask_ai(question, store_id, shop_name):
    """Send question + store data to AI and get answer"""
    context = build_store_context(store_id)

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        max_tokens=300,
        messages=[
            {
                "role": "system",
                "content": f"""You are a friendly business analytics assistant for 
{shop_name.replace('_', ' ').title()}, a local retail store in Mumbai, India.

You have access to the store's sales data and help the owner understand 
their business performance.

Rules:
- Answer in simple, clear English
- You can mix in Hindi/Hinglish words naturally if it feels right
- Always use ₹ for currency
- Be specific with numbers from the data
- Keep answers under 5 lines
- Be encouraging and practical
- If you don't have enough data to answer, say so honestly"""
            },
            {
                "role": "user",
                "content": f"""Store Data:
{context}

Owner's Question: {question}"""
            }
        ]
    )

    return response.choices[0].message.content
import os
from statistics import mean

from dotenv import load_dotenv
from groq import Groq

from app.analytics import (
    get_category_breakdown,
    get_daily_trend,
    get_dead_stock,
    get_low_stock,
    get_recent_revenue_trend,
    get_store_summary,
    get_top_products,
)

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


def _build_growth_signal(weekly_totals):
    if len(weekly_totals) < 2:
        return 0.0
    baseline = mean(weekly_totals[:-1]) or 0
    if baseline <= 0:
        return 0.0
    return (weekly_totals[-1] - baseline) / baseline


def _clamp_forecast(value, recent_week):
    if recent_week <= 0:
        return max(value, 0)
    lower = recent_week * 0.65
    upper = recent_week * 1.45
    return max(lower, min(value, upper))


def build_store_context(store_id):
    """Pull relevant store data to give AI context."""
    summary = get_store_summary(store_id, period="30d")
    top_products = get_top_products(store_id, period="30d", limit=5)
    low_stock = get_low_stock(store_id)
    dead_stock = get_dead_stock(store_id)
    daily_trend = get_daily_trend(store_id, period="7d")
    categories = get_category_breakdown(store_id, period="30d")

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
    """Send question + store data to Groq and get an answer."""
    if client is None:
        raise RuntimeError("GROQ_API_KEY is not configured")

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
- If you don't have enough data to answer, say so honestly""",
            },
            {
                "role": "user",
                "content": f"""Store Data:
{context}

Owner's Question: {question}""",
            },
        ],
    )

    return response.choices[0].message.content


def forecast_next_week_revenue(store_id, shop_name):
    """Predict next week's revenue from recent daily trends, with Groq narrative when available."""
    trend_rows = get_recent_revenue_trend(store_id, days=56)
    if len(trend_rows) < 7:
        raise ValueError("Not enough recent sales data to forecast next week yet.")

    daily_revenue = [float(row.get("revenue") or 0) for row in trend_rows]
    weekly_totals = []
    for start in range(0, len(daily_revenue), 7):
        week_slice = daily_revenue[start:start + 7]
        if len(week_slice) == 7:
            weekly_totals.append(sum(week_slice))

    if not weekly_totals:
        raise ValueError("Not enough full-week sales data to forecast next week yet.")

    recent_week = weekly_totals[-1]
    trailing_average = mean(weekly_totals[-4:]) if len(weekly_totals) >= 4 else mean(weekly_totals)
    growth_signal = _build_growth_signal(weekly_totals[-4:] if len(weekly_totals) >= 4 else weekly_totals)
    projected_value = _clamp_forecast(trailing_average * (1 + (growth_signal * 0.6)), recent_week)
    rounded_forecast = round(projected_value, 2)
    confidence = "medium"
    if len(weekly_totals) >= 6:
        confidence = "high"
    elif len(weekly_totals) < 3:
        confidence = "low"

    explanation = (
        f"Based on the last {len(weekly_totals)} full weeks, "
        f"recent weekly revenue averaged ₹{round(trailing_average, 2)} "
        f"and the latest week closed at ₹{round(recent_week, 2)}."
    )

    if client is not None:
        prompt = (
            f"Store: {shop_name.replace('_', ' ').title()}\n"
            f"Weekly revenue totals: {', '.join('₹' + str(round(v, 2)) for v in weekly_totals[-8:])}\n"
            f"Projected next week revenue: ₹{rounded_forecast}\n"
            f"Confidence: {confidence}\n\n"
            "Write a 3-line owner-friendly explanation in simple English. "
            "Mention the trend direction and one practical action."
        )
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=160,
            messages=[
                {
                    "role": "system",
                    "content": "You explain retail forecasts clearly for small business owners in India.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        explanation = response.choices[0].message.content.strip()

    return {
        "shop_name": shop_name,
        "forecast_revenue": rounded_forecast,
        "recent_week_revenue": round(recent_week, 2),
        "average_weekly_revenue": round(trailing_average, 2),
        "confidence": confidence,
        "weeks_used": len(weekly_totals),
        "explanation": explanation,
    }

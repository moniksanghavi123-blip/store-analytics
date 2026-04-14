from app.database import run_query
from datetime import date, timedelta

def get_date_range(period='7d', start_date=None, end_date=None):
    """Convert period string to start and end dates"""
    today = date.today()
    if start_date and end_date:
        return start_date, end_date
    periods = {
        '7d':  today - timedelta(days=7),
        '30d': today - timedelta(days=30),
        '90d': today - timedelta(days=90),
        '1y':  today - timedelta(days=365),
    }
    return periods.get(period, today - timedelta(days=7)), today

def get_store_summary(store_id, days=7, period='7d',
                      start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    results = run_query('''
        select
            coalesce(sum(gross_revenue), 0)           as total_revenue,
            coalesce(sum(gross_profit), 0)            as total_profit,
            coalesce(sum(quantity_sold), 0)           as total_units,
            round(coalesce(sum(gross_profit) /
                nullif(sum(gross_revenue), 0) * 100, 0), 1) as margin_pct
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
    ''', (store_id, start, end))
    return results[0] if results else {}

def get_top_products(store_id, days=7, limit=5, period='7d',
                     start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query('''
        select
            product_name,
            category,
            sum(quantity_sold)  as total_units,
            sum(gross_revenue)  as total_revenue,
            sum(gross_profit)   as total_profit
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by product_name, category
        order by total_revenue desc
        limit %s
    ''', (store_id, start, end, limit))

def get_low_stock(store_id):
    return run_query('''
        select distinct on (product_name)
            product_name,
            category,
            closing_stock
        from sales_raw
        where store_id = %s
          and closing_stock is not null
        order by product_name, sale_date desc
    ''', (store_id,))

def get_dead_stock(store_id, days=14):
    return run_query('''
        select
            product_name,
            max(sale_date)                    as last_sold,
            current_date - max(sale_date)     as days_since_sold
        from sales_raw
        where store_id = %s
        group by product_name
        having current_date - max(sale_date) > %s
        order by days_since_sold desc
    ''', (store_id, days))

def get_daily_trend(store_id, days=7, period='7d',
                    start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query('''
        select
            sale_date,
            sum(gross_revenue)  as revenue,
            sum(gross_profit)   as profit,
            sum(quantity_sold)  as units
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by sale_date
        order by sale_date asc
    ''', (store_id, start, end))

def get_category_breakdown(store_id, days=7, period='7d',
                            start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query('''
        select
            coalesce(nullif(trim(category), ''), 'general') as category,
            sum(gross_revenue)  as revenue,
            sum(gross_profit)   as profit,
            sum(quantity_sold)  as units
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by coalesce(nullif(trim(category), ''), 'general')
        order by revenue desc
    ''', (store_id, start, end))

def build_summary(store_id, period='7d'):
    summary      = get_store_summary(store_id, period=period)
    top_products = get_top_products(store_id, period=period)
    low_stock    = get_low_stock(store_id)
    dead_stock   = get_dead_stock(store_id)

    top_str = ""
    for i, p in enumerate(top_products, 1):
        top_str += f"{i}. {p['product_name'].title()} — ₹{p['total_revenue']} ({int(p['total_units'])} units)\n"

    low_stock_items = [
        p['product_name'].title()
        for p in low_stock
        if p['closing_stock'] is not None and p['closing_stock'] < 5
    ]
    low_stock_str  = ', '.join(low_stock_items) if low_stock_items else 'None'
    dead_stock_str = ', '.join([p['product_name'].title()
                                for p in dead_stock]) if dead_stock else 'None'

    return {
        "total_revenue": summary.get('total_revenue', 0),
        "total_profit":  summary.get('total_profit', 0),
        "margin_pct":    summary.get('margin_pct', 0),
        "total_units":   summary.get('total_units', 0),
        "top_products":  top_str.strip(),
        "low_stock":     low_stock_str,
        "dead_stock":    dead_stock_str
    }


def get_period_config(period_key):
    period_map = {
        "weekly": {
            "analytics_period": "7d",
            "label": "Weekly",
            "days": 7,
        },
        "monthly": {
            "analytics_period": "30d",
            "label": "Monthly",
            "days": 30,
        },
        "yearly": {
            "analytics_period": "1y",
            "label": "Yearly",
            "days": 365,
        },
    }
    return period_map.get(period_key, period_map["weekly"])


def build_summary_bundle(store_id, period_key="weekly"):
    config = get_period_config(period_key)
    summary = build_summary(store_id, period=config["analytics_period"])
    return {
        **summary,
        "period_key": period_key,
        "period_label": config["label"],
        "days": config["days"],
    }


def get_recent_revenue_trend(store_id, days=56):
    return run_query(
        '''
        select
            sale_date,
            coalesce(sum(gross_revenue), 0) as revenue
        from sales_raw
        where store_id = %s
          and sale_date >= current_date - %s
        group by sale_date
        order by sale_date asc
        ''',
        (store_id, days)
    )

from app.database import run_query

def get_store_summary(store_id, days=7):
    """Get revenue, profit and margin for last N days"""
    results = run_query('''
        select
            sum(gross_revenue)                        as total_revenue,
            sum(gross_profit)                         as total_profit,
            sum(quantity_sold)                        as total_units,
            round(sum(gross_profit) / 
                nullif(sum(gross_revenue), 0) * 100, 1) as margin_pct
        from sales_raw
        where store_id = %s
          and sale_date >= current_date - %s
    ''', (store_id, days))
    return results[0] if results else {}

def get_top_products(store_id, days=7, limit=3):
    """Get top selling products by revenue"""
    return run_query('''
        select
            product_name,
            sum(quantity_sold)  as total_units,
            sum(gross_revenue)  as total_revenue,
            sum(gross_profit)   as total_profit
        from sales_raw
        where store_id = %s
          and sale_date >= current_date - %s
        group by product_name
        order by total_revenue desc
        limit %s
    ''', (store_id, days, limit))

def get_low_stock(store_id):
    """Get products with closing stock below 5"""
    return run_query('''
        select distinct on (product_name)
            product_name,
            closing_stock
        from sales_raw
        where store_id = %s
          and closing_stock is not null
        order by product_name, sale_date desc
    ''', (store_id,))

def get_dead_stock(store_id, days=14):
    """Get products not sold in last N days"""
    return run_query('''
        select
            product_name,
            max(sale_date)                          as last_sold,
            current_date - max(sale_date)           as days_since_sold
        from sales_raw
        where store_id = %s
        group by product_name
        having current_date - max(sale_date) > %s
        order by days_since_sold desc
    ''', (store_id, days))

def get_daily_trend(store_id, days=7):
    """Get daily revenue for last N days"""
    return run_query('''
        select
            sale_date,
            sum(gross_revenue)  as revenue,
            sum(gross_profit)   as profit
        from sales_raw
        where store_id = %s
          and sale_date >= current_date - %s
        group by sale_date
        order by sale_date asc
    ''', (store_id, days))

def get_category_breakdown(store_id, days=7):
    """Get revenue breakdown by category"""
    return run_query('''
        select
            category,
            sum(gross_revenue)  as revenue,
            sum(gross_profit)   as profit,
            sum(quantity_sold)  as units
        from sales_raw
        where store_id = %s
          and sale_date >= current_date - %s
          and category is not null
        group by category
        order by revenue desc
    ''', (store_id, days))

def build_summary(store_id):
    """Build complete summary dict for one store"""
    summary = get_store_summary(store_id)
    top_products = get_top_products(store_id)
    low_stock = get_low_stock(store_id)
    dead_stock = get_dead_stock(store_id)

    # Format top products as readable string
    top_str = ""
    for i, p in enumerate(top_products, 1):
        top_str += f"{i}. {p['product_name'].title()} — ₹{p['total_revenue']} ({int(p['total_units'])} units)\n"

    # Format low stock
    low_stock_items = [
        p['product_name'].title()
        for p in low_stock
        if p['closing_stock'] is not None and p['closing_stock'] < 5
    ]
    low_stock_str = ', '.join(low_stock_items) if low_stock_items else 'None'

    # Format dead stock
    dead_stock_items = [p['product_name'].title() for p in dead_stock]
    dead_stock_str = ', '.join(dead_stock_items) if dead_stock_items else 'None'

    return {
        "total_revenue": summary.get('total_revenue', 0),
        "total_profit": summary.get('total_profit', 0),
        "margin_pct": summary.get('margin_pct', 0),
        "total_units": summary.get('total_units', 0),
        "top_products": top_str.strip(),
        "low_stock": low_stock_str,
        "dead_stock": dead_stock_str
    }
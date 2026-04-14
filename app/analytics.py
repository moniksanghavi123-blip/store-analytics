from datetime import date, timedelta

from app.database import run_query, table_exists


def _safe_float(value):
    return float(value or 0)


def get_date_range(period="7d", start_date=None, end_date=None):
    """Convert period string to start and end dates."""
    today = date.today()
    if start_date and end_date:
        return start_date, end_date
    periods = {
        "1d": today,
        "7d": today - timedelta(days=7),
        "30d": today - timedelta(days=30),
        "90d": today - timedelta(days=90),
        "1y": today - timedelta(days=365),
    }
    return periods.get(period, today - timedelta(days=7)), today


def get_store_summary(store_id, days=7, period="7d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    results = run_query(
        """
        select
            coalesce(sum(gross_revenue), 0) as total_revenue,
            coalesce(sum(gross_profit), 0) as total_profit,
            coalesce(sum(quantity_sold), 0) as total_units,
            round(
                coalesce(sum(gross_profit) / nullif(sum(gross_revenue), 0) * 100, 0),
                1
            ) as margin_pct
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        """,
        (store_id, start, end),
    )
    return results[0] if results else {}


def get_top_products(store_id, days=7, limit=5, period="7d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query(
        """
        select
            product_name,
            category,
            sum(quantity_sold) as total_units,
            sum(gross_revenue) as total_revenue,
            sum(gross_profit) as total_profit
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by product_name, category
        order by total_revenue desc
        limit %s
        """,
        (store_id, start, end, limit),
    )


def get_top_profit_products(store_id, limit=5, period="30d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query(
        """
        select
            product_name,
            category,
            sum(quantity_sold) as total_units,
            sum(gross_revenue) as total_revenue,
            sum(gross_profit) as total_profit,
            round(
                coalesce(sum(gross_profit) / nullif(sum(gross_revenue), 0) * 100, 0),
                1
            ) as margin_pct
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by product_name, category
        order by total_profit desc, total_revenue desc
        limit %s
        """,
        (store_id, start, end, limit),
    )


def get_low_stock(store_id):
    return run_query(
        """
        select distinct on (product_name)
            product_name,
            category,
            closing_stock
        from sales_raw
        where store_id = %s
          and closing_stock is not null
        order by product_name, sale_date desc
        """,
        (store_id,),
    )


def get_dead_stock(store_id, days=14):
    return run_query(
        """
        select
            product_name,
            max(sale_date) as last_sold,
            current_date - max(sale_date) as days_since_sold
        from sales_raw
        where store_id = %s
        group by product_name
        having current_date - max(sale_date) > %s
        order by days_since_sold desc
        """,
        (store_id, days),
    )


def get_daily_trend(store_id, days=7, period="7d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query(
        """
        select
            sale_date,
            sum(gross_revenue) as revenue,
            sum(gross_profit) as profit,
            sum(quantity_sold) as units
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by sale_date
        order by sale_date asc
        """,
        (store_id, start, end),
    )


def get_category_breakdown(store_id, days=7, period="7d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query(
        """
        select
            coalesce(nullif(trim(category), ''), 'general') as category,
            sum(gross_revenue) as revenue,
            sum(gross_profit) as profit,
            sum(quantity_sold) as units
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by coalesce(nullif(trim(category), ''), 'general')
        order by revenue desc
        """,
        (store_id, start, end),
    )


def get_recent_revenue_trend(store_id, days=56):
    return run_query(
        """
        select
            sale_date,
            coalesce(sum(gross_revenue), 0) as revenue
        from sales_raw
        where store_id = %s
          and sale_date >= current_date - %s
        group by sale_date
        order by sale_date asc
        """,
        (store_id, days),
    )


def get_product_velocity(store_id, period="30d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    return run_query(
        """
        with product_days as (
            select
                product_name,
                coalesce(nullif(trim(category), ''), 'general') as category,
                sale_date,
                sum(quantity_sold) as units_sold,
                sum(gross_revenue) as revenue,
                sum(gross_profit) as profit,
                avg(selling_price) as avg_selling_price,
                avg(coalesce(purchase_price, 0)) as avg_purchase_price
            from sales_raw
            where store_id = %s
              and sale_date between %s and %s
            group by product_name, coalesce(nullif(trim(category), ''), 'general'), sale_date
        ),
        latest_stock as (
            select distinct on (product_name)
                product_name,
                closing_stock
            from sales_raw
            where store_id = %s
              and closing_stock is not null
            order by product_name, sale_date desc
        )
        select
            p.product_name,
            p.category,
            sum(p.units_sold) as total_units,
            count(*) as active_days,
            round(avg(p.units_sold), 2) as avg_units_per_active_day,
            round(sum(p.units_sold) / greatest((%s::date - %s::date) + 1, 1), 2) as avg_units_per_day,
            round(sum(p.revenue), 2) as total_revenue,
            round(sum(p.profit), 2) as total_profit,
            round(avg(p.avg_selling_price), 2) as avg_selling_price,
            round(avg(p.avg_purchase_price), 2) as avg_purchase_price,
            ls.closing_stock
        from product_days p
        left join latest_stock ls on ls.product_name = p.product_name
        group by p.product_name, p.category, ls.closing_stock
        order by total_units desc, total_revenue desc
        """,
        (store_id, start, end, store_id, end, start),
    )


def get_reorder_suggestions(store_id, period="30d", start_date=None, end_date=None, target_cover_days=14):
    suggestions = []
    for row in get_product_velocity(store_id, period=period, start_date=start_date, end_date=end_date):
        avg_daily_units = _safe_float(row.get("avg_units_per_day"))
        current_stock = _safe_float(row.get("closing_stock"))
        if avg_daily_units <= 0 or row.get("closing_stock") is None:
            continue
        days_left = round(current_stock / avg_daily_units, 1) if avg_daily_units else None
        reorder_qty = max(round((avg_daily_units * target_cover_days) - current_stock), 0)
        urgency = "healthy"
        if days_left is not None:
            if current_stock <= 0 or days_left <= 3:
                urgency = "urgent"
            elif days_left <= 7:
                urgency = "soon"
        if reorder_qty > 0 and urgency != "healthy":
            suggestions.append(
                {
                    **row,
                    "days_left": days_left,
                    "reorder_qty": reorder_qty,
                    "target_cover_days": target_cover_days,
                    "urgency": urgency,
                }
            )
    return sorted(
        suggestions,
        key=lambda r: (
            0 if r["urgency"] == "urgent" else 1,
            r["days_left"] if r["days_left"] is not None else 9999,
        ),
    )[:6]


def get_stockout_predictions(store_id, period="30d", start_date=None, end_date=None):
    predictions = []
    for row in get_product_velocity(store_id, period=period, start_date=start_date, end_date=end_date):
        avg_daily_units = _safe_float(row.get("avg_units_per_day"))
        current_stock = _safe_float(row.get("closing_stock"))
        if avg_daily_units <= 0 or row.get("closing_stock") is None:
            continue
        days_left = round(current_stock / avg_daily_units, 1)
        if current_stock <= 0 or days_left <= 7:
            predictions.append(
                {
                    **row,
                    "days_left": days_left,
                    "risk": "high" if current_stock <= 0 or days_left <= 3 else "medium",
                }
            )
    return sorted(predictions, key=lambda r: r["days_left"])[:6]


def get_purchase_plan(store_id, period="30d", start_date=None, end_date=None, horizon_days=14):
    plan = []
    for row in get_product_velocity(store_id, period=period, start_date=start_date, end_date=end_date):
        avg_daily_units = _safe_float(row.get("avg_units_per_day"))
        current_stock = _safe_float(row.get("closing_stock"))
        if avg_daily_units <= 0:
            continue
        recommended_units = max(round(avg_daily_units * horizon_days - current_stock), 0)
        if recommended_units <= 0:
            continue
        approx_budget = round(recommended_units * _safe_float(row.get("avg_purchase_price")), 2)
        plan.append(
            {
                **row,
                "recommended_units": recommended_units,
                "horizon_days": horizon_days,
                "approx_budget": approx_budget,
            }
        )
    return sorted(plan, key=lambda r: (r["approx_budget"], r["recommended_units"]), reverse=True)[:8]


def get_price_recommendations(store_id, period="30d", start_date=None, end_date=None):
    rows = get_product_velocity(store_id, period=period, start_date=start_date, end_date=end_date)
    recommendations = []
    overall = get_store_summary(store_id, period=period, start_date=start_date, end_date=end_date)
    store_margin = _safe_float(overall.get("margin_pct"))
    for row in rows:
        revenue = _safe_float(row.get("total_revenue"))
        profit = _safe_float(row.get("total_profit"))
        units = _safe_float(row.get("total_units"))
        avg_sell = _safe_float(row.get("avg_selling_price"))
        avg_buy = _safe_float(row.get("avg_purchase_price"))
        if revenue <= 0 or units < 5 or avg_sell <= 0:
            continue
        margin_pct = round((profit / revenue) * 100, 1) if revenue else 0
        if avg_buy <= 0:
            continue
        target_margin = max(store_margin, 15)
        target_price = round(avg_buy / max(1 - (target_margin / 100), 0.01), 2)
        suggested_raise = round(max(target_price - avg_sell, 0), 2)
        if profit < 0 or margin_pct < max(store_margin - 5, 10):
            recommendations.append(
                {
                    **row,
                    "margin_pct": margin_pct,
                    "target_margin_pct": round(target_margin, 1),
                    "suggested_price": target_price,
                    "suggested_raise": suggested_raise,
                    "priority": "urgent" if profit < 0 or margin_pct <= 5 else "review",
                }
            )
    return sorted(
        recommendations,
        key=lambda r: (
            0 if r["priority"] == "urgent" else 1,
            r["margin_pct"],
            -_safe_float(r.get("total_units")),
        ),
    )[:6]


def get_basket_analysis(store_id, period="30d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    pairs = run_query(
        """
        with product_days as (
            select distinct
                sale_date,
                product_name
            from sales_raw
            where store_id = %s
              and sale_date between %s and %s
        )
        select
            a.product_name as left_product,
            b.product_name as right_product,
            count(*) as co_days
        from product_days a
        join product_days b
          on a.sale_date = b.sale_date
         and a.product_name < b.product_name
        group by a.product_name, b.product_name
        having count(*) >= 2
        order by co_days desc, left_product asc, right_product asc
        limit 6
        """,
        (store_id, start, end),
    )
    return pairs


def get_target_vs_actual(store_id):
    month_start = date.today().replace(day=1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    actual = get_store_summary(store_id, start_date=month_start, end_date=min(date.today(), next_month - timedelta(days=1)))
    goal_rows = run_query(
        """
        select revenue_target, profit_target
        from store_goals
        where store_id = %s
          and goal_month = %s
        limit 1
        """,
        (store_id, month_start),
    ) if table_exists("store_goals") else []
    goal = goal_rows[0] if goal_rows else {}
    revenue_target = _safe_float(goal.get("revenue_target"))
    profit_target = _safe_float(goal.get("profit_target"))
    actual_revenue = _safe_float(actual.get("total_revenue"))
    actual_profit = _safe_float(actual.get("total_profit"))
    return {
        "goal_month": month_start,
        "revenue_target": revenue_target,
        "profit_target": profit_target,
        "actual_revenue": actual_revenue,
        "actual_profit": actual_profit,
        "revenue_progress_pct": round((actual_revenue / revenue_target) * 100, 1) if revenue_target else None,
        "profit_progress_pct": round((actual_profit / profit_target) * 100, 1) if profit_target else None,
    }


def get_owner_store_rollup(phone_number, period="30d", start_date=None, end_date=None):
    if not phone_number:
        return {"stores": [], "store_count": 0, "summary": None}
    start, end = get_date_range(period, start_date, end_date)
    normalized = phone_number.strip().replace("+", "").replace(" ", "")
    stores = run_query(
        """
        select
            s.id,
            s.shop_name,
            s.plan,
            coalesce(sum(sr.gross_revenue), 0) as revenue,
            coalesce(sum(sr.gross_profit), 0) as profit
        from stores s
        left join sales_raw sr
          on sr.store_id = s.id
         and sr.sale_date between %s and %s
        where replace(replace(s.phone_number, '+', ''), ' ', '') = %s
          and coalesce(s.is_active, true) = true
        group by s.id, s.shop_name, s.plan
        order by revenue desc, s.shop_name asc
        """,
        (start, end, normalized),
    )
    summary = {
        "total_revenue": round(sum(_safe_float(s.get("revenue")) for s in stores), 2),
        "total_profit": round(sum(_safe_float(s.get("profit")) for s in stores), 2),
    } if stores else None
    return {
        "stores": stores,
        "store_count": len(stores),
        "summary": summary,
    }


def get_seasonal_insights(store_id, period="90d", start_date=None, end_date=None):
    start, end = get_date_range(period, start_date, end_date)
    weekday_rows = run_query(
        """
        select
            extract(isodow from sale_date) as weekday_num,
            trim(to_char(sale_date, 'Dy')) as weekday_label,
            sum(gross_revenue) as revenue
        from sales_raw
        where store_id = %s
          and sale_date between %s and %s
        group by extract(isodow from sale_date), trim(to_char(sale_date, 'Dy'))
        order by weekday_num asc
        """,
        (store_id, start, end),
    )
    category_rows = get_category_breakdown(store_id, period="30d")
    insights = []
    if weekday_rows:
        best_day = max(weekday_rows, key=lambda row: _safe_float(row.get("revenue")))
        worst_day = min(weekday_rows, key=lambda row: _safe_float(row.get("revenue")))
        insights.append(
            {
                "title": "Peak sales day",
                "detail": f"{best_day['weekday_label']} performs best lately. Plan staff and fresh stock before that day.",
            }
        )
        if best_day["weekday_label"] != worst_day["weekday_label"]:
            insights.append(
                {
                    "title": "Slow day opportunity",
                    "detail": f"{worst_day['weekday_label']} is softer. Run bundles or WhatsApp offers to lift that day.",
                }
            )
    if category_rows:
        top_category = category_rows[0]
        insights.append(
            {
                "title": "Seasonal demand pocket",
                "detail": f"{str(top_category['category']).title()} is leading in the last 30 days. Keep this category deeper in stock.",
            }
        )
    festival_map = {
        1: "New Year and winter restocking",
        3: "Holi demand spikes",
        4: "summer beverages demand",
        5: "pre-monsoon essentials",
        8: "Raksha Bandhan and festive gifting",
        9: "Ganesh festival demand",
        10: "Navratri, Dussehra, and Diwali demand",
        11: "wedding season demand",
        12: "year-end gifting demand",
    }
    current_month_tip = festival_map.get(date.today().month)
    if current_month_tip:
        insights.append(
            {
                "title": "Calendar reminder",
                "detail": f"This month often sees {current_month_tip}. Plan promotions and replenishment a little early.",
            }
        )
    return insights[:4]


def build_summary(store_id, period="7d"):
    summary = get_store_summary(store_id, period=period)
    top_products = get_top_products(store_id, period=period)
    low_stock = get_low_stock(store_id)
    dead_stock = get_dead_stock(store_id)

    top_str = ""
    for i, product in enumerate(top_products, 1):
        top_str += (
            f"{i}. {product['product_name'].title()} — ₹{product['total_revenue']} "
            f"({int(product['total_units'])} units)\n"
        )

    low_stock_items = [
        item["product_name"].title()
        for item in low_stock
        if item["closing_stock"] is not None and item["closing_stock"] < 5
    ]
    low_stock_str = ", ".join(low_stock_items) if low_stock_items else "None"
    dead_stock_str = ", ".join([item["product_name"].title() for item in dead_stock]) if dead_stock else "None"

    return {
        "total_revenue": summary.get("total_revenue", 0),
        "total_profit": summary.get("total_profit", 0),
        "margin_pct": summary.get("margin_pct", 0),
        "total_units": summary.get("total_units", 0),
        "top_products": top_str.strip(),
        "low_stock": low_stock_str,
        "dead_stock": dead_stock_str,
    }


def get_period_config(period_key):
    period_map = {
        "daily": {"analytics_period": "1d", "label": "Daily", "days": 1},
        "weekly": {"analytics_period": "7d", "label": "Weekly", "days": 7},
        "monthly": {"analytics_period": "30d", "label": "Monthly", "days": 30},
        "yearly": {"analytics_period": "1y", "label": "Yearly", "days": 365},
    }
    return period_map.get(period_key, period_map["weekly"])


def build_summary_bundle(store_id, period_key="weekly"):
    config = get_period_config(period_key)
    summary = build_summary(store_id, period=config["analytics_period"])
    reorder = get_reorder_suggestions(store_id, period="30d")
    stockout = get_stockout_predictions(store_id, period="30d")
    return {
        **summary,
        "period_key": period_key,
        "period_label": config["label"],
        "days": config["days"],
        "reorder_summary": ", ".join([item["product_name"].title() for item in reorder[:3]]) or "None",
        "stockout_summary": ", ".join([item["product_name"].title() for item in stockout[:3]]) or "None",
    }


def build_dashboard_insights(store_id, phone_number=None, period="30d", start_date=None, end_date=None):
    return {
        "top_profit_products": get_top_profit_products(store_id, period=period, start_date=start_date, end_date=end_date),
        "reorder_suggestions": get_reorder_suggestions(store_id, period=period, start_date=start_date, end_date=end_date),
        "stockout_predictions": get_stockout_predictions(store_id, period=period, start_date=start_date, end_date=end_date),
        "purchase_plan": get_purchase_plan(store_id, period=period, start_date=start_date, end_date=end_date),
        "price_recommendations": get_price_recommendations(store_id, period=period, start_date=start_date, end_date=end_date),
        "basket_analysis": get_basket_analysis(store_id, period=period, start_date=start_date, end_date=end_date),
        "target_progress": get_target_vs_actual(store_id),
        "owner_rollup": get_owner_store_rollup(phone_number, period=period, start_date=start_date, end_date=end_date),
        "seasonal_insights": get_seasonal_insights(store_id, period="90d"),
    }

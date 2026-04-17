WITH base_kpi AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue_rows,
        COUNT(DISTINCT SalesOrderID) AS distinct_orders,
        SUM(Revenue) AS total_revenue,
        COUNT(DISTINCT CustomerID) AS customer_count,
        SUM(Revenue) * 1.0 / NULLIF(COUNT(DISTINCT SalesOrderID), 0) AS AOV,
        MAX(CAST(OrderDate AS date)) AS latest_order_date
    FROM dbo.vw_fact_sales
),

daily_sales AS (
    SELECT
        CAST(OrderDate AS date) AS order_date,
        SUM(Revenue) AS daily_revenue
    FROM dbo.vw_fact_sales
    GROUP BY CAST(OrderDate AS date)
),

latest_revenue_check AS (
    SELECT TOP 1
        order_date,
        daily_revenue,
        AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS prev_7day_avg
    FROM daily_sales
    ORDER BY order_date DESC
),

category_check AS (
    SELECT
        COUNT(*) AS zero_revenue_category_count
    FROM (
        SELECT
            ProductCategory
        FROM dbo.vw_fact_sales
        GROUP BY ProductCategory
        HAVING SUM(Revenue) = 0
    ) z
)

SELECT
    b.total_rows,
    b.null_revenue_rows,
    b.distinct_orders,
    b.total_revenue,
    b.customer_count,
    b.AOV,
    b.latest_order_date,

    r.order_date AS latest_revenue_date,
    r.daily_revenue,
    r.prev_7day_avg,

    CASE
        WHEN r.prev_7day_avg IS NULL THEN NULL
        ELSE r.daily_revenue * 1.0 / NULLIF(r.prev_7day_avg, 0)
    END AS revenue_ratio,

    CASE
        WHEN r.prev_7day_avg IS NULL THEN 0
        WHEN r.daily_revenue < r.prev_7day_avg * 0.5 THEN 1
        ELSE 0
    END AS revenue_drop_flag,

    c.zero_revenue_category_count,

    CASE
        WHEN c.zero_revenue_category_count > 0 THEN 1
        ELSE 0
    END AS zero_revenue_category_flag

FROM base_kpi b
CROSS JOIN latest_revenue_check r
CROSS JOIN category_check c;
GO
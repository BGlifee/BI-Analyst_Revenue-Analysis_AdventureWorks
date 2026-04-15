SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue_rows,
    COUNT(DISTINCT SalesOrderID) AS distinct_orders,
    SUM(Revenue) AS total_revenue,
    COUNT(DISTINCT CustomerID) AS customer_count,
    SUM(Revenue) / COUNT(DISTINCT SalesOrderID) AS AOV
FROM dbo.vw_fact_sales;
GO
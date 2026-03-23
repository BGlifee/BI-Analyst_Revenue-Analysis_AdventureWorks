USE AdventureWorks2025;
GO

SELECT
    YEAR(OrderDate) AS OrderYear,
    MONTH(OrderDate) AS OrderMonth,
    SUM(Revenue) AS MonthlyRevenue,
    COUNT(DISTINCT SalesOrderID) AS Orders,
    SUM(Revenue) / COUNT(DISTINCT SalesOrderID) AS AOV
FROM dbo.vw_fact_sales
GROUP BY YEAR(OrderDate), MONTH(OrderDate)
ORDER BY OrderYear, OrderMonth;
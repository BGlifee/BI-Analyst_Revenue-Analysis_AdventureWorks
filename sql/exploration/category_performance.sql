USE AdventureWorks2025;
GO

SELECT
    ProductCategory,
    SUM(Revenue) AS TotalRevenue,
    COUNT(DISTINCT SalesOrderID) AS Orders,
    COUNT(DISTINCT CustomerID) AS Customers
FROM dbo.vw_fact_sales
GROUP BY ProductCategory
ORDER BY TotalRevenue DESC;
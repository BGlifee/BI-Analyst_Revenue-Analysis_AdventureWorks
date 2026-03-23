USE AdventureWorks2025;
GO

SELECT
    TerritoryGroup,
    TerritoryName,
    SUM(Revenue) AS TotalRevenue,
    COUNT(DISTINCT SalesOrderID) AS Orders,
    COUNT(DISTINCT CustomerID) AS Customers
FROM dbo.vw_fact_sales
GROUP BY TerritoryGroup, TerritoryName
ORDER BY TotalRevenue DESC;
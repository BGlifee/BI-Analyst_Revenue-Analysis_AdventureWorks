-- Validate key business metrics

-- Total Revenue
SELECT SUM(Revenue) AS TotalRevenue
FROM dbo.vw_fact_sales;

-- Total Orders
SELECT COUNT(DISTINCT SalesOrderID) AS OrderCount
FROM dbo.vw_fact_sales;

-- Total Customers
SELECT COUNT(DISTINCT CustomerID) AS CustomerCount
FROM dbo.vw_fact_sales;

-- AOV
SELECT
    SUM(Revenue) / COUNT(DISTINCT SalesOrderID) AS AOV
FROM dbo.vw_fact_sales;
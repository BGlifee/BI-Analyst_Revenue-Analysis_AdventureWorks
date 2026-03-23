USE AdventureWorks2025;
GO

-- 1. Null profiling
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN h.CustomerID IS NULL THEN 1 ELSE 0 END) AS null_customerid,
    SUM(CASE WHEN d.ProductID IS NULL THEN 1 ELSE 0 END) AS null_productid,
    SUM(CASE WHEN d.LineTotal IS NULL THEN 1 ELSE 0 END) AS null_linetotal,
    SUM(CASE WHEN d.OrderQty IS NULL THEN 1 ELSE 0 END) AS null_orderqty
FROM Sales.SalesOrderHeader h
JOIN Sales.SalesOrderDetail d
    ON h.SalesOrderID = d.SalesOrderID;

-- 2. Duplicate check
SELECT
    SalesOrderDetailID,
    COUNT(*) AS duplicate_count
FROM Sales.SalesOrderDetail
GROUP BY SalesOrderDetailID
HAVING COUNT(*) > 1;

-- 3. Invalid quantity / revenue checks
SELECT
    COUNT(*) AS invalid_qty_rows
FROM Sales.SalesOrderDetail
WHERE OrderQty <= 0;

SELECT
    COUNT(*) AS invalid_revenue_rows
FROM Sales.SalesOrderDetail
WHERE LineTotal <= 0 OR UnitPrice < 0;

-- 4. Missing category mapping
SELECT
    COUNT(*) AS missing_category_rows
FROM Sales.SalesOrderDetail d
LEFT JOIN Production.Product p
    ON d.ProductID = p.ProductID
LEFT JOIN Production.ProductSubcategory ps
    ON p.ProductSubcategoryID = ps.ProductSubcategoryID
LEFT JOIN Production.ProductCategory pc
    ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE pc.Name IS NULL;
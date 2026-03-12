USE AdventureWorks2025;
GO
SELECT TOP 10 *
FROM INFORMATION_SCHEMA.TABLES;

SELECT
    h.SalesOrderID,
    h.OrderDate,
    h.CustomerID,
    d.ProductID,
    d.OrderQty,
    d.UnitPrice,
    d.LineTotal
FROM Sales.SalesOrderHeader h
JOIN Sales.SalesOrderDetail d
ON h.SalesOrderID = d.SalesOrderID
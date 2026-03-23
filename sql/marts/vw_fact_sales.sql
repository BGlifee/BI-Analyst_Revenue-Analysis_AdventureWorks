-- Purpose: Clean fact table for revenue analysis
-- Grain: One row per sales order line
-- Key Metrics: Revenue (LineTotal), OrderQty

USE AdventureWorks2025;
GO

CREATE OR ALTER VIEW dbo.vw_fact_sales AS
SELECT
    h.SalesOrderID,
    d.SalesOrderDetailID,
    h.CustomerID,
    h.TerritoryID,
    d.ProductID,

    CAST(h.OrderDate AS date) AS OrderDate,

    d.OrderQty,
    d.UnitPrice,
    d.UnitPriceDiscount,
    d.LineTotal AS Revenue,

    p.Name AS ProductName,
    pc.Name AS ProductCategory,

    st.Name AS TerritoryName,
    st.[Group] AS TerritoryGroup

FROM Sales.SalesOrderHeader h
JOIN Sales.SalesOrderDetail d
    ON h.SalesOrderID = d.SalesOrderID
LEFT JOIN Production.Product p
    ON d.ProductID = p.ProductID
LEFT JOIN Production.ProductSubcategory ps
    ON p.ProductSubcategoryID = ps.ProductSubcategoryID
LEFT JOIN Production.ProductCategory pc
    ON ps.ProductCategoryID = pc.ProductCategoryID
LEFT JOIN Sales.SalesTerritory st
    ON h.TerritoryID = st.TerritoryID

WHERE d.LineTotal IS NOT NULL
  AND d.OrderQty > 0;

  
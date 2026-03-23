-- Purpose: Product dimension for category analysis

USE AdventureWorks2025;
GO

CREATE OR ALTER VIEW dbo.vw_dim_product AS
SELECT
    p.ProductID,
    p.Name AS ProductName,
    p.ProductNumber,
    p.Color,
    p.ListPrice,

    ps.Name AS ProductSubcategory,
    pc.Name AS ProductCategory

FROM Production.Product p
LEFT JOIN Production.ProductSubcategory ps
    ON p.ProductSubcategoryID = ps.ProductSubcategoryID
LEFT JOIN Production.ProductCategory pc
    ON ps.ProductCategoryID = pc.ProductCategoryID;
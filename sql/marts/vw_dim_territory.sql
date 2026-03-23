-- Purpose: Territory dimension for regional analysis

USE AdventureWorks2025;
GO

CREATE OR ALTER VIEW dbo.vw_dim_territory AS
SELECT
    TerritoryID,
    Name AS TerritoryName,
    CountryRegionCode,
    [Group] AS TerritoryGroup
FROM Sales.SalesTerritory;
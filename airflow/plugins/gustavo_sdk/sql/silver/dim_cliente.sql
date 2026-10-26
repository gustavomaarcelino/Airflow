CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_silver.dim_cliente` AS

SELECT
    customerid,
    personid,
    storeid,
    territoryid

FROM `ecommercegustavo-data-500420.adventureworks_bronze.customer`;
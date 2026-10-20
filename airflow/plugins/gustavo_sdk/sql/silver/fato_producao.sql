CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_silver.fato_producao` AS

SELECT
    workorderid,
    productid,
    orderqty,
    scrappedqty,
    startdate,
    enddate,
    duedate

FROM `ecommercegustavo-data-500420.adventureworks_bronze.workorder`;
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_silver.fato_vendas` AS

SELECT
    d.salesorderdetailid,
    d.salesorderid,
    d.productid,
    d.orderqty,
    d.unitprice,
    d.unitpricediscount,
    h.orderdate,
    h.customerid

FROM `ecommercegustavo-data-500420.adventureworks_bronze.salesorderdetail` AS d
INNER JOIN `ecommercegustavo-data-500420.adventureworks_bronze.salesorderheader` AS h
    ON h.salesorderid = d.salesorderid;
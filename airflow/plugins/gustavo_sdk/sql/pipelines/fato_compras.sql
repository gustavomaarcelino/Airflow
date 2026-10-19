CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_silver.fato_compras` AS

SELECT
    d.purchaseorderdetailid,
    d.purchaseorderid,
    d.productid,
    d.orderqty,
    d.unitprice,
    d.receivedqty,
    d.rejectedqty,
    d.duedate,
    h.orderdate,
    h.vendorid

FROM `ecommercegustavo-data-500420.adventureworks_bronze.purchaseorderdetail` AS d
INNER JOIN `ecommercegustavo-data-500420.adventureworks_bronze.purchaseorderheader` AS h
    ON d.purchaseorderid = h.purchaseorderid;
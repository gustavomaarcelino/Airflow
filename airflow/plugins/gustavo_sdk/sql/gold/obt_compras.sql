
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_gold.obt_compras` AS

SELECT
    -- Identificação da compra
    fc.purchaseorderdetailid,
    fc.purchaseorderid,
    fc.orderdate,
    fc.duedate,

    -- Informações da compra
    fc.productid,
    fc.businessentityid,
    fc.orderqty,
    fc.unitprice,
    fc.receivedqty,
    fc.rejectedqty,

    -- Informações do produto
    dp.name AS produto_nome,
    dp.productnumber,
    dp.color,
    dp.listprice,
    dp.size,
    dp.productline,
    dp.style,

    -- Informações do fornecedor
    df.accountnumber,
    df.name AS fornecedor_nome,
    df.creditrating,
    df.preferredvendorstatus,
    df.activeflag,
    df.purchasingwebserviceurl

FROM `ecommercegustavo-data-500420.adventureworks_silver.fato_compras` AS fc

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp
    ON fc.productid = dp.productid

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_fornecedor` AS df
    ON fc.businessentityid = df.businessentityid;
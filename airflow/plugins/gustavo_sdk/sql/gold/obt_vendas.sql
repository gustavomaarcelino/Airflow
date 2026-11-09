
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_gold.obt_vendas` AS

SELECT
    -- Identificação da venda
    fv.salesorderdetailid,
    fv.salesorderid,
    fv.orderdate,

    -- Informações da venda
    fv.productid,
    fv.orderqty,
    fv.unitprice,
    fv.unitpricediscount,
    fv.customerid,

    -- Informações do produto
    dp.name AS produto_nome,
    dp.productnumber,
    dp.color,
    dp.listprice,
    dp.size,
    dp.productline,
    dp.style,

    -- Informações do cliente
    dc.personid,
    dc.storeid,
    dc.territoryid

FROM `ecommercegustavo-data-500420.adventureworks_silver.fato_vendas` AS fv

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp
    ON fv.productid = dp.productid

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_cliente` AS dc
    ON fv.customerid = dc.customerid;

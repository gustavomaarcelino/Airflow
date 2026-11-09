
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_gold.obt_producao` AS

SELECT
    -- Identificação da ordem
    fp.workorderid,
    fp.productid,

    -- Informações da produção
    fp.orderqty,
    fp.scrappedqty,
    fp.startdate,
    fp.enddate,
    fp.duedate,

    -- Informações do produto
    dp.name AS produto_nome,
    dp.productnumber,
    dp.color,
    dp.listprice,
    dp.size,
    dp.productline,
    dp.style

FROM `ecommercegustavo-data-500420.adventureworks_silver.fato_producao` AS fp

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp
    ON fp.productid = dp.productid;

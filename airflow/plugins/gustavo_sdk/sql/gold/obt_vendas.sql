
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_gold.obt_vendas` AS

SELECT
    -- Identificação da venda
    fv.salesorderdetailid AS id_item_pedido,
    fv.salesorderid AS id_pedido,
    fv.orderdate AS data_pedido,

    -- Informações da venda
    fv.productid AS id_produto,
    fv.orderqty AS quantidade,
    fv.unitprice AS preco_unitario,
    fv.unitpricediscount AS percentual_desconto,
    fv.customerid AS id_cliente,

    -- Informações do produto
    dp.name AS nome_produto,
    dp.productnumber AS numero_produto,
    dp.color AS cor,
    dp.listprice AS preco_tabela,
    dp.size AS tamanho,
    dp.productline AS linha_produto,
    dp.style AS estilo,

    -- Informações do cliente
    dc.personid AS id_pessoa,
    dc.storeid AS id_loja,
    dc.territoryid AS id_territorio

FROM `ecommercegustavo-data-500420.adventureworks_silver.fato_vendas` AS fv

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp
    ON fv.productid = dp.productid

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_cliente` AS dc
    ON fv.customerid = dc.customerid;

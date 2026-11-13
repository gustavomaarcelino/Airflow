
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_gold.obt_compras` AS

SELECT
    -- Identificação da compra
    fc.purchaseorderdetailid AS id_item_pedido_compra,
    fc.purchaseorderid AS id_pedido_compra,
    fc.orderdate AS data_pedido_compra,
    fc.duedate AS data_prevista,

    -- Informações da compra
    fc.productid AS id_produto,
    fc.businessentityid AS id_fornecedor,
    fc.orderqty AS quantidade_pedida,
    fc.unitprice AS preco_unitario,
    fc.receivedqty AS quantidade_recebida,
    fc.rejectedqty AS quantidade_rejeitada,

    -- Informações do produto
    dp.name AS nome_produto,
    dp.productnumber AS numero_produto,
    dp.color AS cor,
    dp.listprice AS preco_tabela,
    dp.size AS tamanho,
    dp.productline AS linha_produto,
    dp.style AS estilo,

    -- Informações do fornecedor
    df.accountnumber AS numero_conta,
    df.name AS nome_fornecedor,
    df.creditrating AS classificacao_credito,
    df.preferredvendorstatus AS fornecedor_preferencial,
    df.activeflag AS fornecedor_ativo,
    df.purchasingwebserviceurl AS url_servico_compras

FROM `ecommercegustavo-data-500420.adventureworks_silver.fato_compras` AS fc

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp
    ON fc.productid = dp.productid

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_fornecedor` AS df
    ON fc.businessentityid = df.businessentityid;

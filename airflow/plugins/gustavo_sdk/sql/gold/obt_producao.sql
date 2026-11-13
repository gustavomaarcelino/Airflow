
CREATE OR REPLACE TABLE
  `ecommercegustavo-data-500420.adventureworks_gold.obt_producao` AS

SELECT
    -- Identificação da ordem
    fp.workorderid AS id_ordem_producao,
    fp.productid AS id_produto,

    -- Informações da produção
    fp.orderqty AS quantidade_produzir,
    fp.scrappedqty AS quantidade_refugada,
    fp.startdate AS data_inicio,
    fp.enddate AS data_fim,
    fp.duedate AS data_prevista,

    -- Informações do produto
    dp.name AS nome_produto,
    dp.productnumber AS numero_produto,
    dp.color AS cor,
    dp.listprice AS preco_tabela,
    dp.size AS tamanho,
    dp.productline AS linha_produto,
    dp.style AS estilo

FROM `ecommercegustavo-data-500420.adventureworks_silver.fato_producao` AS fp

LEFT JOIN `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp
    ON fp.productid = dp.productid;

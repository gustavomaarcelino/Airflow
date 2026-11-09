CREATE OR REPLACE TABLE
`ecommercegustavo-data-500420.adventureworks_gold.obt_produto`
AS

WITH vendas AS (
  SELECT
        productid,
        SUM(orderqty) AS quantidade_vendida,
        COUNT(DISTINCT salesorderid) AS pedidos_distintos,
        SUM(orderqty * unitprice) AS valor_total_vendido
        FROM `adventureworks_silver.fato_vendas`
        GROUP BY productid
),

compras AS (
    SELECT
        productid,
        SUM(orderqty) AS quantidade_comprada,
        COUNT(DISTINCT purchaseorderid) AS pedidos_compra_distintos,
        SUM(orderqty * unitprice) AS valor_total_comprado
        FROM `adventureworks_silver.fato_compras`
        GROUP BY productid
),

producao AS (
  SELECT
      productid,
      SUM(orderqty) AS quantidade_registrada_ordens_producao,
      SUM(scrappedqty) AS quantidade_descartada,
      COUNT(DISTINCT workorderid) AS ordens_producao_distintas
      FROM `adventureworks_silver.fato_producao`
      GROUP BY productid
),

clientes AS (
    SELECT
        fv.productid,
        COUNT(DISTINCT dc.customerid) AS clientes_distintos
    FROM `adventureworks_silver.fato_vendas` AS fv
    INNER JOIN `adventureworks_silver.dim_cliente` AS dc
        ON fv.customerid = dc.customerid
    GROUP BY fv.productid
),

fornecedores AS (
  SELECT fc.productid,
  COUNT(DISTINCT df.businessentityid) AS fornecedores_distintos
  FROM `adventureworks_silver.fato_compras` AS fc
  INNER JOIN `adventureworks_silver.dim_fornecedor` AS df
  ON fc.businessentityid = df.businessentityid
  GROUP BY fc.productid
)

SELECT
    dp.*,

    -- Métricas de vendas
COALESCE(v.quantidade_vendida, 0) AS quantidade_vendida,
COALESCE(v.pedidos_distintos, 0) AS pedidos_distintos,
COALESCE(v.valor_total_vendido, 0) AS valor_total_vendido,

-- Métricas de compras
COALESCE(c.quantidade_comprada, 0) AS quantidade_comprada,
COALESCE(c.pedidos_compra_distintos, 0) AS pedidos_compra_distintos,
COALESCE(c.valor_total_comprado, 0) AS valor_total_comprado,

-- Métricas de produção
COALESCE(p.quantidade_registrada_ordens_producao, 0) AS quantidade_registrada_ordens_producao,
COALESCE(p.quantidade_descartada, 0) AS quantidade_descartada,
COALESCE(p.ordens_producao_distintas, 0) AS ordens_producao_distintas,

-- Métricas de clientes
COALESCE(cl.clientes_distintos, 0) AS clientes_distintos,

-- Métricas de fornecedores
COALESCE(f.fornecedores_distintos, 0) AS fornecedores_distintos

FROM `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS dp

LEFT JOIN vendas AS v
    ON dp.productid = v.productid

LEFT JOIN compras AS c
    ON dp.productid = c.productid

LEFT JOIN producao AS p
    ON dp.productid = p.productid

LEFT JOIN clientes AS cl
ON dp.productid = cl.productid

LEFT JOIN fornecedores AS f
ON dp.productid = f.productid
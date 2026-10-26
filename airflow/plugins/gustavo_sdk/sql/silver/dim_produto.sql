CREATE OR REPLACE TABLE
    `ecommercegustavo-data-500420.adventureworks_silver.dim_produto` AS
SELECT
    productid,
    name,
    productnumber,
    color,
    listprice,
    size,
    productline,
    style,
FROM `ecommercegustavo-data-500420.adventureworks_bronze.product`;

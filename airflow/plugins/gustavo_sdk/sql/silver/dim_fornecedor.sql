CREATE OR REPLACE TABLE `ecommercegustavo-data-500420.adventureworks_silver.dim_fornecedor` AS

SELECT
    businessentityid,
    accountnumber,
    name,
    creditrating,
    preferredvendorstatus,
    activeflag,
    purchasingwebserviceurl
FROM `ecommercegustavo-data-500420.adventureworks_bronze.vendor`;
/****************************************************************************************
 Script Name : silver_erp_product_inv_restock_data.sql
 Layer       : Silver
 Source      : bronze.erp_product_inv_restock_data
 Target      : silver.erp_product_inv_restock_data
 Purpose     : Clean and validate ERP product inventory restock data
****************************************************************************************/

-- Drop existing table for safe re-runs
IF OBJECT_ID('silver.erp_product_inv_restock_data','U') IS NOT NULL
    DROP TABLE silver.erp_product_inv_restock_data;

-- Create Silver layer table
CREATE TABLE silver.erp_product_inv_restock_data(
    product_code        NVARCHAR(30),
    warehouse_id        NVARCHAR(30),
    reorder_level       INT,
    last_restock_date   DATE,
    restock_qty         INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);

PRINT 'Cleaning and loading silver.erp_product_inv_restock_data...';

-- Load cleaned data from Bronze layer
INSERT INTO silver.erp_product_inv_restock_data (
    product_code,
    warehouse_id,
    reorder_level,
    last_restock_date,
    restock_qty
)
SELECT
    product_code,
    warehouse_id,

    -- Ensure non-negative reorder level
    CASE
        WHEN reorder_level >= 0 THEN reorder_level
        ELSE 10
    END AS reorder_level,

    -- Remove future or invalid restock dates
    CASE
        WHEN last_restock_date IS NOT NULL
         AND last_restock_date <= CAST(GETDATE() AS DATE)
        THEN last_restock_date
        ELSE NULL
    END AS last_restock_date,

    -- Ensure positive restock quantity
    CASE
        WHEN restock_qty > 0 THEN restock_qty
        ELSE 0
    END AS restock_qty

FROM bronze.erp_product_inv_restock_data
WHERE
    product_code IS NOT NULL AND LEN(TRIM(product_code)) > 0
    AND warehouse_id IS NOT NULL AND LEN(TRIM(warehouse_id)) > 0;

-- Log load count
PRINT 'Completed: ' + CAST(@@ROWCOUNT AS VARCHAR) +
      ' inventory records cleaned and loaded.';

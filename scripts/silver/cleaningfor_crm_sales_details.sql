/****************************************************************************************
 Script Name : silver_crm_sales_details.sql
 Layer       : Silver
 Source      : bronze.crm_sales_details
 Target      : silver.crm_sales_details
 Purpose     : Clean and validate CRM sales transaction data
****************************************************************************************/

-- Drop existing Silver table for re-runnable execution
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

-- Create Silver layer table
CREATE TABLE silver.crm_sales_details(
    sale_id          NVARCHAR(30),
    customer_id      NVARCHAR(30),
    product_id       NVARCHAR(30),
    quantity         INT,
    sale_date        DATE,
    total_amount     INT,
    dwh_create_date  DATETIME2 DEFAULT GETDATE()
);

PRINT 'Cleaning and loading silver.crm_sales_details...';

-- Load cleaned data from Bronze layer
INSERT INTO silver.crm_sales_details (
    sale_id,
    customer_id,
    product_id,
    quantity,
    sale_date,
    total_amount
)
SELECT
    sale_id,
    customer_id,
    product_id,

    -- Ensure quantity is positive
    CASE
        WHEN quantity > 0 THEN quantity
        ELSE 1
    END AS quantity,

    -- Remove future or invalid sale dates
    CASE
        WHEN sale_date IS NOT NULL
         AND sale_date <= CAST(GETDATE() AS DATE)
        THEN sale_date
        ELSE NULL
    END AS sale_date,

    -- Remove negative sales amounts
    CASE
        WHEN total_amount >= 0 THEN total_amount
        ELSE 0
    END AS total_amount

FROM bronze.crm_sales_details
WHERE
    sale_id IS NOT NULL AND LEN(TRIM(sale_id)) > 0
    AND customer_id IS NOT NULL AND LEN(TRIM(customer_id)) > 0
    AND product_id IS NOT NULL AND LEN(TRIM(product_id)) > 0;

-- Log load count
PRINT 'Completed: ' + CAST(@@ROWCOUNT AS VARCHAR) +
      ' sales records cleaned and loaded.';

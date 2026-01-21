/****************************************************************************************
 Script Name   : silver_crm_product_info.sql
 Layer         : Silver (Cleaned & Standardized Data)
 Source Table  : bronze.crm_product_info
 Target Table  : silver.crm_product_info
 Purpose       :
    - Clean, standardize, and validate CRM product data
    - Apply business rules for product attributes
    - Prepare high-quality data for analytics and downstream consumption
****************************************************************************************/

-- Drop the Silver table if it already exists to allow idempotent execution
IF OBJECT_ID('silver.crm_product_info','U') IS NOT NULL
    DROP TABLE silver.crm_product_info;

-- Create Silver layer table with cleaned and structured schema
CREATE TABLE silver.crm_product_info(
    product_id        NVARCHAR(30),   -- Unique identifier for each product
    product_name      NVARCHAR(50),   -- Cleaned product name (proper case)
    category          NVARCHAR(50),   -- Standardized product category
    price             INT,            -- Validated product price (> 0)
    stock_qty         INT,            -- Validated stock quantity (>= 0)
    supplier          NVARCHAR(30),   -- Cleaned supplier name
    dwh_create_date   DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);

PRINT 'Starting data cleaning and load for silver.crm_product_info...';

-- Insert cleaned and transformed data from Bronze layer into Silver layer
INSERT INTO silver.crm_product_info (
    product_id,
    product_name,
    category,
    price,
    stock_qty,
    supplier
)
SELECT
    -- Ensure product_id is present and trimmed
    product_id,

    /* 
       Clean product name:
       - Trim whitespace
       - Convert to Proper Case
       - Replace NULL or empty values with 'Unnamed Product'
    */
    CASE
        WHEN product_name IS NOT NULL AND LEN(TRIM(product_name)) > 0
            THEN UPPER(LEFT(TRIM(product_name), 1)) +
                 LOWER(SUBSTRING(TRIM(product_name), 2, LEN(TRIM(product_name))))
        ELSE 'Unnamed Product'
    END AS product_name,

    /*
       Standardize product categories:
       - Map similar or inconsistent values to a controlled vocabulary
       - Handle NULL values explicitly
    */
    CASE
        WHEN LOWER(TRIM(category)) LIKE '%electron%' THEN 'Electronics'
        WHEN LOWER(TRIM(category)) LIKE '%fashion%'
          OR LOWER(TRIM(category)) LIKE '%cloth%'   THEN 'Fashion'
        WHEN LOWER(TRIM(category)) LIKE '%grocery%'
          OR LOWER(TRIM(category)) LIKE '%food%'    THEN 'Groceries'
        WHEN LOWER(TRIM(category)) LIKE '%home%'
          OR LOWER(TRIM(category)) LIKE '%kitchen%' THEN 'Home & Kitchen'
        WHEN LOWER(TRIM(category)) LIKE '%sport%'   THEN 'Sports'
        WHEN category IS NULL                       THEN 'Uncategorized'
        ELSE TRIM(category)
    END AS category,

    /*
       Validate price:
       - Keep only positive values
       - Set invalid (zero or negative) prices to NULL
    */
    CASE
        WHEN price > 0 THEN price
        ELSE NULL
    END AS price,

    /*
       Validate stock quantity:
       - Negative stock values are set to 0
    */
    CASE
        WHEN stock_qty >= 0 THEN stock_qty
        ELSE 0
    END AS stock_qty,

    /*
       Clean supplier name:
       - Trim whitespace
       - Convert to Proper Case
       - Replace NULL or empty values with 'Unknown Supplier'
    */
    CASE
        WHEN supplier IS NOT NULL AND LEN(TRIM(supplier)) > 0
            THEN UPPER(LEFT(TRIM(supplier), 1)) +
                 LOWER(SUBSTRING(TRIM(supplier), 2, LEN(TRIM(supplier))))
        ELSE 'Unknown Supplier'
    END AS supplier

FROM bronze.crm_product_info
WHERE
    -- Enforce presence of a valid product_id
    product_id IS NOT NULL
    AND LEN(TRIM(product_id)) > 0;

-- Log number of records successfully loaded into Silver layer
PRINT 'Completed: ' + CAST(@@ROWCOUNT AS VARCHAR) +
      ' product records cleaned and loaded into silver.crm_product_info.';

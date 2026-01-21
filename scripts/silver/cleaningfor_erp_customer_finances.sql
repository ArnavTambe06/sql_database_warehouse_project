/****************************************************************************************
 Script Name : silver_erp_cust_finances.sql
 Layer       : Silver
 Source      : bronze.erp_cust_finances
 Target      : silver.erp_cust_finances
 Purpose     : Clean and standardize ERP customer financial data
****************************************************************************************/

-- Drop existing table to allow re-runs
IF OBJECT_ID('silver.erp_cust_finances','U') IS NOT NULL
    DROP TABLE silver.erp_cust_finances;

-- Create Silver layer table
CREATE TABLE silver.erp_cust_finances(
    cust_code         NVARCHAR(30),
    region            NVARCHAR(50),
    loyalty_points    INT,
    credit_limit      INT,
    account_status    NVARCHAR(30),
    dwh_create_date   DATETIME2 DEFAULT GETDATE()
);

PRINT 'Cleaning and loading silver.erp_cust_finances...';

-- Load cleaned data from Bronze layer
INSERT INTO silver.erp_cust_finances (
    cust_code,
    region,
    loyalty_points,
    credit_limit,
    account_status
)
SELECT
    cust_code,

    -- Standardize region values
    CASE
        WHEN LOWER(TRIM(region)) LIKE '%north%america%' THEN 'North America'
        WHEN LOWER(TRIM(region)) LIKE '%europe%'        THEN 'Europe'
        WHEN LOWER(TRIM(region)) LIKE '%asia%'          THEN 'Asia'
        WHEN LOWER(TRIM(region)) LIKE '%middle%east%'   THEN 'Middle East'
        WHEN LOWER(TRIM(region)) LIKE '%latin%america%'
          OR LOWER(TRIM(region)) LIKE '%south%america%' THEN 'Latin America'
        WHEN LOWER(TRIM(region)) LIKE '%africa%'        THEN 'Africa'
        WHEN LOWER(TRIM(region)) LIKE '%oceania%'
          OR LOWER(TRIM(region)) LIKE '%australia%'     THEN 'Oceania'
        WHEN region IS NULL                             THEN 'Unknown Region'
        ELSE TRIM(region)
    END AS region,

    -- Ensure non-negative loyalty points
    CASE
        WHEN loyalty_points >= 0 THEN loyalty_points
        ELSE 0
    END AS loyalty_points,

    -- Ensure non-negative credit limit
    CASE
        WHEN credit_limit >= 0 THEN credit_limit
        ELSE 0
    END AS credit_limit,

    -- Normalize account status values
    CASE
        WHEN LOWER(TRIM(account_status)) IN ('active','act','a')       THEN 'Active'
        WHEN LOWER(TRIM(account_status)) IN ('suspended','suspend','s') THEN 'Suspended'
        WHEN LOWER(TRIM(account_status)) IN ('inactive','inact','i')   THEN 'Inactive'
        WHEN LOWER(TRIM(account_status)) IN ('closed','close','c')     THEN 'Closed'
        WHEN account_status IS NULL                                    THEN 'Active'
        ELSE TRIM(account_status)
    END AS account_status

FROM bronze.erp_cust_finances
WHERE
    cust_code IS NOT NULL
    AND LEN(TRIM(cust_code)) > 0;

-- Log load count
PRINT 'Completed: ' + CAST(@@ROWCOUNT AS VARCHAR) +
      ' customer finance records cleaned and loaded.';

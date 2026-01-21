/****************************************************************************************
 Script Name : silver_erp_warehouse_logistics_data.sql
 Layer       : Silver
 Source      : bronze.erp_warehouse_logistics_data
 Target      : silver.erp_warehouse_logistics_data
 Purpose     : Clean and validate ERP warehouse logistics data
****************************************************************************************/

-- Drop existing table for safe re-runs
IF OBJECT_ID('silver.erp_warehouse_logistics_data','U') IS NOT NULL
    DROP TABLE silver.erp_warehouse_logistics_data;

-- Create Silver layer table
CREATE TABLE silver.erp_warehouse_logistics_data(
    warehouse_id           NVARCHAR(30),
    city                   NVARCHAR(50),
    capacity               INT,
    current_utilization    INT,
    manager                NVARCHAR(20),
    dwh_create_date        DATETIME2 DEFAULT GETDATE()
);

PRINT 'Cleaning and loading silver.erp_warehouse_logistics_data...';

-- Load cleaned data from Bronze layer
INSERT INTO silver.erp_warehouse_logistics_data (
    warehouse_id,
    city,
    capacity,
    current_utilization,
    manager
)
SELECT
    warehouse_id,

    -- Clean city name
    CASE
        WHEN city IS NOT NULL AND LEN(TRIM(city)) > 0
            THEN UPPER(LEFT(TRIM(city),1)) +
                 LOWER(SUBSTRING(TRIM(city),2,LEN(TRIM(city))))
        ELSE 'Unknown City'
    END AS city,

    -- Ensure positive capacity
    CASE
        WHEN capacity > 0 THEN capacity
        ELSE 10000
    END AS capacity,

    -- Ensure utilization is within capacity
    CASE
        WHEN current_utilization >= 0
         AND current_utilization <= capacity THEN current_utilization
        WHEN current_utilization > capacity  THEN capacity
        ELSE 0
    END AS current_utilization,

    -- Clean manager name
    CASE
        WHEN manager IS NOT NULL AND LEN(TRIM(manager)) > 0
            THEN UPPER(LEFT(TRIM(manager),1)) +
                 LOWER(SUBSTRING(TRIM(manager),2,LEN(TRIM(manager))))
        ELSE 'Unknown Manager'
    END AS manager

FROM bronze.erp_warehouse_logistics_data
WHERE
    warehouse_id IS NOT NULL
    AND LEN(TRIM(warehouse_id)) > 0;

-- Log load count
PRINT 'Completed: ' + CAST(@@ROWCOUNT AS VARCHAR) +
      ' warehouse records cleaned and loaded.';

/*
Full loading of all the tables.
If the Staging Table is not Created.
Then
1. In this first create Temporary tables to Load the data into them and after all checks load them into the main tables.
2. Before loading into main table cast the date data-type to avoid the date data-type error.
3. Drop the Temporary Tables.
*/




CREATE OR ALTER PROCEDURE bronze.usp_FullLoadAllTables
    @DebugMode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RecordCount INT;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @ErrorMessage NVARCHAR(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        IF @DebugMode = 1
            PRINT 'Starting full load process at: ' + CONVERT(VARCHAR, @StartTime, 120);

        -- 1. Load CRM Customer Info (Direct approach with temp table)
        IF @DebugMode = 1
            PRINT 'Loading CRM customer info...';
        
        -- Create temporary table
        CREATE TABLE #temp_customer_info (
            customer_id NVARCHAR(30),
            customer_name NVARCHAR(20),
            email NVARCHAR(30),
            phone NVARCHAR(20),
            city NVARCHAR(20),
            join_date NVARCHAR(20)
        );
        
        -- Bulk insert into temp table
        BULK INSERT #temp_customer_info
        FROM 'C:\Project\Data_Engineer_Projects\Data-Warehouse-Project\sql-data-warehouse-project\datasets\sources_crm\customer_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        
        -- Load into final table
        TRUNCATE TABLE bronze.crm_customer_info;
        INSERT INTO bronze.crm_customer_info
        SELECT 
            customer_id,
            customer_name,
            email,
            phone,
            city,
            CASE WHEN ISDATE(join_date) = 1 THEN CAST(join_date AS DATE) ELSE NULL END
        FROM #temp_customer_info;
        
        SET @RecordCount = @@ROWCOUNT;
        DROP TABLE #temp_customer_info;
        
        IF @DebugMode = 1
            PRINT 'Loaded ' + CAST(@RecordCount AS VARCHAR) + ' customer records';

        -- 2. Load CRM Product Info
        IF @DebugMode = 1
            PRINT 'Loading CRM product info...';
        
        CREATE TABLE #temp_product_info (
            product_id NVARCHAR(30),
            product_name NVARCHAR(50),
            category NVARCHAR(50),
            price NVARCHAR(20),
            stock_qty NVARCHAR(20),
            supplier NVARCHAR(30)
        );
        
        BULK INSERT #temp_product_info
        FROM 'C:\Project\Data_Engineer_Projects\Data-Warehouse-Project\sql-data-warehouse-project\datasets\sources_crm\product_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        
        TRUNCATE TABLE bronze.crm_product_info;
        INSERT INTO bronze.crm_product_info
        SELECT 
            product_id,
            product_name,
            category,
            CAST(price AS INT),
            CAST(stock_qty AS INT),
            supplier
        FROM #temp_product_info;
        
        SET @RecordCount = @@ROWCOUNT;
        DROP TABLE #temp_product_info;
        
        IF @DebugMode = 1
            PRINT 'Loaded ' + CAST(@RecordCount AS VARCHAR) + ' product records';

        -- 3. Load CRM Sales Details
        IF @DebugMode = 1
            PRINT 'Loading CRM sales details...';
        
        CREATE TABLE #temp_sales_details (
            sale_id NVARCHAR(30),
            customer_id NVARCHAR(30),
            product_id NVARCHAR(30),
            quantity NVARCHAR(20),
            sale_date NVARCHAR(20),
            total_amount NVARCHAR(20)
        );
        
        BULK INSERT #temp_sales_details
        FROM 'C:\Project\Data_Engineer_Projects\Data-Warehouse-Project\sql-data-warehouse-project\datasets\sources_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        
        TRUNCATE TABLE bronze.crm_sales_details;
        INSERT INTO bronze.crm_sales_details
        SELECT 
            sale_id,
            customer_id,
            product_id,
            CAST(quantity AS INT),
            CASE WHEN ISDATE(sale_date) = 1 THEN CAST(sale_date AS DATE) ELSE NULL END,
            CAST(total_amount AS INT)
        FROM #temp_sales_details;
        
        SET @RecordCount = @@ROWCOUNT;
        DROP TABLE #temp_sales_details;
        
        IF @DebugMode = 1
            PRINT 'Loaded ' + CAST(@RecordCount AS VARCHAR) + ' sales records';

        -- 4. Load ERP Customer Finances
        IF @DebugMode = 1
            PRINT 'Loading ERP customer finances...';
        
        CREATE TABLE #temp_cust_finances (
            cust_code NVARCHAR(30),
            region NVARCHAR(50),
            loyalty_points NVARCHAR(20),
            credit_limit NVARCHAR(20),
            account_status NVARCHAR(30)
        );
        
        BULK INSERT #temp_cust_finances
        FROM 'C:\Project\Data_Engineer_Projects\Data-Warehouse-Project\sql-data-warehouse-project\datasets\sources_erp\cust_finances.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        
        TRUNCATE TABLE bronze.erp_cust_finances;
        INSERT INTO bronze.erp_cust_finances
        SELECT 
            cust_code,
            region,
            CAST(loyalty_points AS INT),
            CAST(credit_limit AS INT),
            account_status
        FROM #temp_cust_finances;
        
        SET @RecordCount = @@ROWCOUNT;
        DROP TABLE #temp_cust_finances;
        
        IF @DebugMode = 1
            PRINT 'Loaded ' + CAST(@RecordCount AS VARCHAR) + ' customer finance records';

        -- 5. Load ERP Warehouse Logistics
        IF @DebugMode = 1
            PRINT 'Loading ERP warehouse logistics...';
        
        CREATE TABLE #temp_warehouse_logistics (
            warehouse_id NVARCHAR(30),
            city NVARCHAR(50),
            capacity NVARCHAR(20),
            current_utilization NVARCHAR(20),
            manager NVARCHAR(20)
        );
        
        BULK INSERT #temp_warehouse_logistics
        FROM 'C:\Project\Data_Engineer_Projects\Data-Warehouse-Project\sql-data-warehouse-project\datasets\sources_erp\warehouse_logistics_data.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        
        TRUNCATE TABLE bronze.erp_warehouse_logistics_data;
        INSERT INTO bronze.erp_warehouse_logistics_data
        SELECT 
            warehouse_id,
            city,
            CAST(capacity AS INT),
            CAST(current_utilization AS INT),
            manager
        FROM #temp_warehouse_logistics;
        
        SET @RecordCount = @@ROWCOUNT;
        DROP TABLE #temp_warehouse_logistics;
        
        IF @DebugMode = 1
            PRINT 'Loaded ' + CAST(@RecordCount AS VARCHAR) + ' warehouse records';

        -- 6. Load ERP Product Inventory
        IF @DebugMode = 1
            PRINT 'Loading ERP product inventory...';
        
        CREATE TABLE #temp_product_inventory (
            product_code NVARCHAR(30),
            warehouse_id NVARCHAR(30),
            reorder_level NVARCHAR(20),
            last_restock_date NVARCHAR(20),
            restock_qty NVARCHAR(20)
        );
        
        BULK INSERT #temp_product_inventory
        FROM 'C:\Project\Data_Engineer_Projects\Data-Warehouse-Project\sql-data-warehouse-project\datasets\sources_erp\product_inv_restock_data.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        
        TRUNCATE TABLE bronze.erp_product_inv_restock_data;
        INSERT INTO bronze.erp_product_inv_restock_data
        SELECT 
            product_code,
            warehouse_id,
            CAST(reorder_level AS INT),
            CASE WHEN ISDATE(last_restock_date) = 1 THEN CAST(last_restock_date AS DATE) ELSE NULL END,
            CAST(restock_qty AS INT)
        FROM #temp_product_inventory;
        
        SET @RecordCount = @@ROWCOUNT;
        DROP TABLE #temp_product_inventory;
        
        IF @DebugMode = 1
            PRINT 'Loaded ' + CAST(@RecordCount AS VARCHAR) + ' inventory records';

        COMMIT TRANSACTION;

        DECLARE @EndTime DATETIME = GETDATE();
        DECLARE @DurationSeconds INT = DATEDIFF(SECOND, @StartTime, @EndTime);
        
        PRINT 'SUCCESS: Full load completed in ' + CAST(@DurationSeconds AS VARCHAR) + ' seconds at ' + CONVERT(VARCHAR, @EndTime, 120);

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Clean up any remaining temp tables
        IF OBJECT_ID('tempdb..#temp_customer_info') IS NOT NULL
            DROP TABLE #temp_customer_info;
        IF OBJECT_ID('tempdb..#temp_product_info') IS NOT NULL
            DROP TABLE #temp_product_info;
        IF OBJECT_ID('tempdb..#temp_sales_details') IS NOT NULL
            DROP TABLE #temp_sales_details;
        IF OBJECT_ID('tempdb..#temp_cust_finances') IS NOT NULL
            DROP TABLE #temp_cust_finances;
        IF OBJECT_ID('tempdb..#temp_warehouse_logistics') IS NOT NULL
            DROP TABLE #temp_warehouse_logistics;
        IF OBJECT_ID('tempdb..#temp_product_inventory') IS NOT NULL
            DROP TABLE #temp_product_inventory;
        
        SELECT @ErrorMessage = ERROR_MESSAGE();
        PRINT 'ERROR: Full load failed - ' + @ErrorMessage;
        RAISERROR (@ErrorMessage, 16, 1);
    END CATCH;
END;
GO

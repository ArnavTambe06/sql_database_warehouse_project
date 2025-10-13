/*
=============================================
Verification Load to address issues like: 
=============================================
Checks for critical data issues like:
  Missing customer references in sales
  Invalid dates
  Orphaned records between systems

Early Problem Detection.
*/




CREATE OR ALTER PROCEDURE bronze.usp_VerifyDataLoad
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '=== DATA LOAD VERIFICATION REPORT ===';
    PRINT '';
    
    -- Record counts
    SELECT 
        'crm_customer_info' AS table_name, 
        COUNT(*) AS record_count,
        MIN(join_date) AS min_date,
        MAX(join_date) AS max_date
    FROM bronze.crm_customer_info
    UNION ALL
    SELECT 'crm_product_info', COUNT(*), NULL, NULL FROM bronze.crm_product_info
    UNION ALL
    SELECT 'crm_sales_details', COUNT(*), MIN(sale_date), MAX(sale_date) FROM bronze.crm_sales_details
    UNION ALL
    SELECT 'erp_cust_finances', COUNT(*), NULL, NULL FROM bronze.erp_cust_finances
    UNION ALL
    SELECT 'erp_warehouse_logistics', COUNT(*), NULL, NULL FROM bronze.erp_warehouse_logistics_data
    UNION ALL
    SELECT 'erp_product_inventory', COUNT(*), MIN(last_restock_date), MAX(last_restock_date) FROM bronze.erp_product_inv_restock_data;
    
    PRINT '';
    PRINT '=== DATA QUALITY CHECKS ===';
    
    -- Check for NULL dates
    DECLARE @NullDates INT;
    SELECT @NullDates = COUNT(*) 
    FROM bronze.crm_customer_info 
    WHERE join_date IS NULL;
    
    IF @NullDates > 0
        PRINT 'WARNING: ' + CAST(@NullDates AS VARCHAR) + ' customer records have NULL join dates';
    ELSE
        PRINT 'PASS: All customer join dates are valid';
    
    -- Check for orphaned sales
    SELECT @NullDates = COUNT(*)
    FROM bronze.crm_sales_details s
    LEFT JOIN bronze.crm_customer_info c ON s.customer_id = c.customer_id
    WHERE c.customer_id IS NULL;
    
    IF @NullDates > 0
        PRINT 'WARNING: ' + CAST(@NullDates AS VARCHAR) + ' sales records have missing customer references';
    ELSE
        PRINT 'PASS: All sales records have valid customer references';
        
    -- Check for orphaned products in sales
    SELECT @NullDates = COUNT(*)
    FROM bronze.crm_sales_details s
    LEFT JOIN bronze.crm_product_info p ON s.product_id = p.product_id
    WHERE p.product_id IS NULL;
    
    IF @NullDates > 0
        PRINT 'WARNING: ' + CAST(@NullDates AS VARCHAR) + ' sales records have missing product references';
    ELSE
        PRINT 'PASS: All sales records have valid product references';
END;
GO

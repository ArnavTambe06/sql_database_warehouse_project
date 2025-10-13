/*
Creating one "Master" load to both the procedures:
Load and Verification.
*/




CREATE OR ALTER PROCEDURE bronze.usp_MasterDataLoad
    @DebugMode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '=== STARTING MASTER DATA LOAD PROCESS ===';
    PRINT '';
    
    -- Execute the full load
    EXEC bronze.usp_FullLoadAllTables @DebugMode = @DebugMode;
    
    PRINT '';
    
    -- Execute verification
    EXEC bronze.usp_VerifyDataLoad;
    
    PRINT '';
    PRINT '=== MASTER DATA LOAD COMPLETED ===';
END;
GO



-- Run the complete process
EXEC bronze.usp_MasterDataLoad;

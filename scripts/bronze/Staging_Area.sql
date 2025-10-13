/*
================================
This File/Step is Optional
================================
Creating Staging Area/Tables to avoid data-type errors and to it make more efficient.
*/



-- Staging tables for CRM
CREATE TABLE bronze.staging_customer_info (
    customer_id NVARCHAR(30),
    customer_name NVARCHAR(20),
    email NVARCHAR(30),
    phone NVARCHAR(20),
    city NVARCHAR(20),
    join_date NVARCHAR(20)
);

CREATE TABLE bronze.staging_product_info (
    product_id NVARCHAR(30),
    product_name NVARCHAR(50),
    category NVARCHAR(50),
    price NVARCHAR(20),
    stock_qty NVARCHAR(20),
    supplier NVARCHAR(30)
);

CREATE TABLE bronze.staging_sales_details (
    sale_id NVARCHAR(30),
    customer_id NVARCHAR(30),
    product_id NVARCHAR(30),
    quantity NVARCHAR(20),
    sale_date NVARCHAR(20),
    total_amount NVARCHAR(20)
);

-- Staging tables for ERP
CREATE TABLE bronze.staging_cust_finances (
    cust_code NVARCHAR(30),
    region NVARCHAR(50),
    loyalty_points NVARCHAR(20),
    credit_limit NVARCHAR(20),
    account_status NVARCHAR(30)
);

CREATE TABLE bronze.staging_warehouse_logistics (
    warehouse_id NVARCHAR(30),
    city NVARCHAR(50),
    capacity NVARCHAR(20),
    current_utilization NVARCHAR(20),
    manager NVARCHAR(20)
);

CREATE TABLE bronze.staging_product_inventory (
    product_code NVARCHAR(30),
    warehouse_id NVARCHAR(30),
    reorder_level NVARCHAR(20),
    last_restock_date NVARCHAR(20),
    restock_qty NVARCHAR(20)
);

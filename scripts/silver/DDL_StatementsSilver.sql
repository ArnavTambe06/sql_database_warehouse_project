/*

-------------------------------------------------
Silver Layer
-------------------------------------------------

Silver Layer Contanis the Clean Data
To Store First create the tables in Silver layer

*/




IF OBJECT_ID ('silver.crm_customer_info','U') IS NOT NULL
	DROP TABLE silver.crm_customer_info;
CREATE TABLE silver.crm_customer_info(
	customer_id NVARCHAR(30),
	customer_name NVARCHAR(20),
	email NVARCHAR(30),
	phone NVARCHAR(30),
	city NVARCHAR(20),
	join_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.crm_product_info','U') IS NOT NULL
	DROP TABLE silver.crm_product_info;
CREATE TABLE  silver.crm_product_info(
	product_id NVARCHAR(30),
	product_name NVARCHAR(50),
	category NVARCHAR(50),
	price INT,
	stock_qty INT,
	supplier NVARCHAR(30),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sale_id NVARCHAR(30),
	customer_id NVARCHAR(30),
	product_id NVARCHAR(30),
	quantity INT,
	sale_date DATE,
	total_amount INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.erp_cust_finances','U') IS NOT NULL
	DROP TABLE silver.erp_cust_finances;
CREATE TABLE silver.erp_cust_finances(
cust_code NVARCHAR(30),
region NVARCHAR(50),
loyalty_points INT,
credit_limit INT,
account_status NVARCHAR(30),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_warehouse_logistics_data','U') IS NOT NULL
	DROP TABLE silver.erp_warehouse_logistics_data;
CREATE TABLE silver.erp_warehouse_logistics_data(
warehouse_id NVARCHAR(30),
city NVARCHAR(50),
capacity INT,
current_utilization INT,
manager NVARCHAR(20),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.erp_product_inv_restock_data','U') IS NOT NULL
	DROP TABLE silver.erp_product_inv_restock_data;
CREATE TABLE silver.erp_product_inv_restock_data(
product_code NVARCHAR(30),
warehouse_id NVARCHAR(30),
reorder_level INT,
last_restock_date DATE,
restock_qty INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

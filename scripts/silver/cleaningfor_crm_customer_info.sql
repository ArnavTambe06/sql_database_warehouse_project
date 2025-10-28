/*

-------------------------------------------------
Cleaning Data and Inserting into Silver Layer
-------------------------------------------------

In this the data in crm_customer_info will be transformed and load.
*/


INSERT INTO silver.crm_customer_info (
    customer_id, customer_name, email, phone, city
)
SELECT 
    customer_id,
    -- Clean customer name: trim and proper case
    UPPER(LEFT(customer_name, 1)) + LOWER(SUBSTRING(customer_name, 2, LEN(customer_name))) AS customer_name,
    -- Clean email: lowercase and trim
    LOWER(LTRIM(RTRIM(email))) AS email,
    -- Clean phone: remove non-numeric characters and ensure string format
    CASE 
        WHEN ISNUMERIC(phone) = 1 AND LEN(phone) <= 15 THEN phone
        ELSE NULL 
    END AS phone,
    -- Clean city: proper case
    UPPER(LEFT(city, 1)) + LOWER(SUBSTRING(city, 2, LEN(city))) AS city
    -- Validate date
    /*CASE 
        WHEN ISDATE(join_date) = 1 THEN CAST(join_date AS DATE)
        ELSE NULL 
    END AS join_date*/
FROM bronze.crm_customer_info
WHERE customer_id IS NOT NULL;


--Check for Data Duplication in Primary Key
select customer_id,
count (*)
from bronze.crm_customer_info group by customer_id having count(*) > 1;

/*
If there is Data Duplication.
select *, ROWNUMBER() OVER (PARTITION BY customer_id ORDER BY creation_date DESC //example) as flag_last
from bronze.crm_customer_info
where customer_id = //duplicateID
*/


--Check for unwanted spaces
select customer_name
from bronze.crm_customer_info
where customer_name != TRIM(customer_name);

--Solution
select TRIM(customer_name) as customer_name from bronze.crm_customer_info;







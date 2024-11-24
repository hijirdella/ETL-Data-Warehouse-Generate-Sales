    -- Step 2: Insert or update into dim_product
    -- Description: Insert new product data into the product dimension if they don't already exist.
    --              Update existing product data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_product AS 
    SELECT DISTINCT src.product_id, src.product_name, src.category, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    TRUNCATE TABLE dwh.dim_product;

    INSERT INTO dwh.dim_product (product_id, product_name, category, last_update)
    SELECT DISTINCT src.product_id, src.product_name, src.category, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;


    -- Step 3: Insert or update into dim_store
    -- Description: Insert new store data into the store dimension if they don't already exist.
    --              Update existing store data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_store AS 
    SELECT DISTINCT src.store_id, src.store_name, src.city, src.state, src.country, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    TRUNCATE TABLE dwh.dim_store;

    INSERT INTO dwh.dim_store (store_id, store_name, city, state, country, last_update)
    SELECT DISTINCT src.store_id, src.store_name, src.city, src.state, src.country, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;


    -- Step 4: Insert or update into dim_time
    -- Description: Insert new time data into the time dimension if they don't already exist.
    --              Update existing time data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_time AS 
    SELECT DISTINCT src.time_id, src.date, src.day_of_week, src.month, src.year, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    TRUNCATE TABLE dwh.dim_time;

    INSERT INTO dwh.dim_time (time_id, date, day_of_week, month, year, last_update)
    SELECT DISTINCT src.time_id, src.date, src.day_of_week, src.month, src.year, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;


    -- Step 5: Insert or update into  dim_sales_name
    -- Description: Insert new sales name data into the sales name dimension if they don't already exist.
    --              Update existing sales name data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_sales_name AS 
    SELECT DISTINCT src.sales_name_id, src.sales_name, src.sales_age, src.sales_gender, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    TRUNCATE TABLE dwh.dim_sales_name;

    INSERT INTO dwh.dim_sales_name (sales_name_id, sales_name, sales_age, sales_gender, last_update)
    SELECT DISTINCT src.sales_name_id, src.sales_name, src.sales_age, src.sales_gender, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;
	
	--step 6: Buat Tabel fact_sales_transaction Jika Belum Ada
CREATE TABLE IF NOT EXISTS dwh.fact_sales_transaction AS
SELECT DISTINCT
    src.sale_id,
    src.store_id,
    src.sales_name_id,
	src.time_id,
    src.date,
    src.product_id,
    src.quantity,
    src.price,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS last_update
FROM stg.stg_sales_transaction AS src;

--1. Tambahkan Kolom time_id pada fact_sales_transaction
ALTER TABLE dwh.fact_sales_transaction ADD COLUMN time_id INT;
2: Perbarui Nilai time_id Berdasarkan Tabel dim_time






	--Membersihkan dan Memasukkan Data ke fact_sales_transaction
	-- Truncate table to remove existing data
TRUNCATE TABLE dwh.fact_sales_transaction;

-- Insert new data from the staging table
INSERT INTO dwh.fact_sales_transaction (
    sale_id,
    store_id,
    sales_name_id,
    date,
    product_id,
    quantity,
    price,
    total_sales_amount,
    created_at,
    last_update
)
SELECT DISTINCT
    src.sale_id,
    src.store_id,
    src.sales_name_id,
    src.date,
    src.product_id,
    src.quantity,
    src.price,
    src.quantity * src.price AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS last_update
FROM stg.stg_sales_transaction AS src;


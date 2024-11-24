-- Sample Store Procedure
CREATE OR REPLACE PROCEDURE dwh.generate_sales()
LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Step 1: Truncate and Insert into stg SCD Type 1
    -- Description: Clear the staging table and populate it with data from the source table.
    TRUNCATE TABLE stg.stg_sales_transaction;
    INSERT INTO stg.stg_sales_transaction 
    SELECT * FROM public.sales_transaction;
	
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
	
	-- Step 6: Insert into fact SCD Type 2
    -- Description: Insert new sales transactions into the fact table if they don't already exist.
	INSERT INTO dwh.fact_sales_transaction
		(sale_id, store_id, sales_name_id, product_id, time_id, date, quantity, price)
	SELECT
		src.sale_id,
		src.store_id,
		src.sales_name_id,
		src.product_id,
		src.time_id,
		src.date,
		src.quantity,
		src.price
	FROM
		stg.stg_sales_transaction AS src
	WHERE NOT EXISTS (
		SELECT 1
		FROM dwh.fact_sales_transaction AS fact
		WHERE 
			COALESCE(fact.sale_id, 0) = COALESCE(src.sale_id, 0) AND
			COALESCE(fact.store_id, 0) = COALESCE(src.store_id, 0) AND
			COALESCE(fact.sales_name_id, 0) = COALESCE(src.sales_name_id, 0) AND
			COALESCE(fact.product_id, 0) = COALESCE(src.product_id, 0) AND
			COALESCE(fact.time_id, 0) = COALESCE(src.time_id, 0) AND
			COALESCE(fact.date, CURRENT_DATE) = COALESCE(src.date, CURRENT_DATE) AND
			COALESCE(fact.quantity, 0) = COALESCE(src.quantity, 0) AND
			COALESCE(fact.price, 0) = COALESCE(src.price, 0)
	);


	
	-- Step 5: Truncate and Insert into dm SCD Type 1
	--Langkah Pemrosesan (ETL)
	-- 1. Membersihkan data lama dm_sales_by_store
	TRUNCATE TABLE dm.dm_sales_by_store;


	-- Memasukkan data terbaru
	INSERT INTO dm.dm_sales_by_store
	SELECT 
		s.store_id,
		s.store_name,
		s.city,
		s.state,
		s.country,
		SUM(f.quantity) AS total_quantity,
		SUM(f.quantity * f.price) AS total_sales_amount, -- Perhitungan langsung
		CURRENT_TIMESTAMP AS created_at
	FROM dwh.fact_sales_transaction f
	JOIN dwh.dim_store s ON f.store_id = s.store_id
	WHERE f.sale_id IN (SELECT DISTINCT sale_id FROM stg.stg_sales_transaction)
	GROUP BY s.store_id, s.store_name, s.city, s.state, s.country;


	-- 2. Membersihkan data lama dm_sales_by_product

	-- Membersihkan data lama
	TRUNCATE TABLE dm.dm_sales_by_product;

	-- Memasukkan data terbaru
	INSERT INTO dm.dm_sales_by_product
	SELECT 
		p.product_id,
		p.product_name,
		p.category,
		SUM(f.quantity) AS total_quantity,
		SUM(f.quantity * f.price) AS total_sales_amount, -- Perhitungan langsung
		CURRENT_TIMESTAMP AS created_at
	FROM dwh.fact_sales_transaction f
	JOIN dwh.dim_product p ON f.product_id = p.product_id
	WHERE f.sale_id IN (SELECT DISTINCT sale_id FROM stg.stg_sales_transaction)
	GROUP BY p.product_id, p.product_name, p.category;


    -- 3. Membersihkan data lama dm_sales_by_time
	-- Membersihkan data lama
	-- Membersihkan data lama
	TRUNCATE TABLE dm.dm_sales_by_time;

	-- Memasukkan data terbaru ke dm.dm_sales_by_time
	-- Memasukkan data terbaru ke dm.dm_sales_by_time
	INSERT INTO dm.dm_sales_by_time
	SELECT 
		t.time_id, -- Masukkan time_id sebagai kolom pertama
		t.date,
		t.day_of_week,
		t.month,
		t.year,
		SUM(f.quantity) AS total_quantity,
		SUM(f.quantity * f.price) AS total_sales_amount, -- Perhitungan langsung
		CURRENT_TIMESTAMP AS created_at
	FROM dwh.fact_sales_transaction f
	JOIN dwh.dim_time t ON f.time_id = t.time_id -- Gunakan time_id untuk join
	WHERE f.sale_id IN (SELECT DISTINCT sale_id FROM stg.stg_sales_transaction)
	GROUP BY t.time_id, t.date, t.day_of_week, t.month, t.year;




	
	-- 4. Membersihkan data lama dm_sales_by_salesperson
	-- Membersihkan data lama
	TRUNCATE TABLE dm.dm_sales_by_salesperson;

	-- Memasukkan data terbaru
	INSERT INTO dm.dm_sales_by_salesperson
	SELECT 
		sp.sales_name_id,
		sp.sales_name,
		sp.sales_age,
		sp.sales_gender,
		SUM(f.quantity) AS total_quantity,
		SUM(f.quantity * f.price) AS total_sales_amount, -- Perhitungan langsung
		CURRENT_TIMESTAMP AS created_at
	FROM dwh.fact_sales_transaction f
	JOIN dwh.dim_sales_name sp ON f.sales_name_id = sp.sales_name_id
	WHERE f.sale_id IN (SELECT DISTINCT sale_id FROM stg.stg_sales_transaction)
	GROUP BY sp.sales_name_id, sp.sales_name, sp.sales_age, sp.sales_gender;


	
	-- Membersihkan data lama
	TRUNCATE TABLE dm.dm_sales_summary;

	-- Memasukkan data terbaru
	INSERT INTO dm.dm_sales_summary
	SELECT 
		f.sale_id,
		s.store_name,
		t.date,
		t.month,
		t.year,
		p.product_name,
		p.category,
		sp.sales_name,
		f.quantity,
		f.price,
		f.quantity * f.price AS total_sales_amount, -- Perhitungan langsung
		CURRENT_TIMESTAMP AS created_at
	FROM dwh.fact_sales_transaction f
	JOIN dwh.dim_store s ON f.store_id = s.store_id
	JOIN dwh.dim_time t ON f.time_id = t.time_id
	JOIN dwh.dim_product p ON f.product_id = p.product_id
	JOIN dwh.dim_sales_name sp ON f.sales_name_id = sp.sales_name_id
	WHERE f.sale_id IN (SELECT DISTINCT sale_id FROM stg.stg_sales_transaction);

END;
$procedure$;



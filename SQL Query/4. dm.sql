--1. Tabel dm.dm_sales_by_store
--Tabel ini berisi total penjualan berdasarkan toko untuk analisis performa toko.
CREATE TABLE IF NOT EXISTS dm.dm_sales_by_store AS
SELECT 
    s.store_id,
    s.store_name,
    s.city,
    s.state,
    s.country,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_store s ON f.store_id = s.store_id
GROUP BY s.store_id, s.store_name, s.city, s.state, s.country;

--Langkah Pemrosesan (ETL)
--Setiap tabel data mart dapat di-refresh secara berkala menggunakan query berikut:
--Membersihkan data lama dan mengisi ulang dengan data terbaru.
TRUNCATE TABLE dm.dm_sales_by_store;
INSERT INTO dm.dm_sales_by_store
SELECT 
    s.store_id,
    s.store_name,
    s.city,
    s.state,
    s.country,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_store s ON f.store_id = s.store_id
GROUP BY s.store_id, s.store_name, s.city, s.state, s.country;


--2. Tabel dm.dm_sales_by_product
--Tabel ini menganalisis total penjualan berdasarkan produk.
CREATE TABLE IF NOT EXISTS dm.dm_sales_by_product AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category;

--Membersihkan data lama dan mengisi ulang dengan data terbaru.
--TRUNCATE TABLE dm.dm_sales_by_product;

INSERT INTO dm.dm_sales_by_product
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category;


--3. Tabel dm.dm_sales_by_time
--Tabel ini berisi total penjualan berdasarkan periode waktu untuk analisis tren.
CREATE TABLE IF NOT EXISTS dm.dm_sales_by_time AS
SELECT 
    t.time_id,
    t.date,
    t.day_of_week,
    t.month,
    t.year,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_time t ON f.time_id = t.time_id
GROUP BY t.time_id, t.date, t.day_of_week, t.month, t.year;

--Membersihkan data lama dan mengisi ulang dengan data terbaru.
TRUNCATE TABLE dm.dm_sales_by_time;

INSERT INTO dm.dm_sales_by_time
SELECT 
    t.time_id,
    t.date,
    t.day_of_week,
    t.month,
    t.year,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_time t ON f.time_id = t.time_id
GROUP BY t.time_id, t.date, t.day_of_week, t.month, t.year;


--4. Tabel dm.dm_sales_by_salesperson
--Tabel ini berisi performa tim penjualan, total transaksi, dan total penjualan yang dihasilkan setiap penjual.
CREATE TABLE IF NOT EXISTS dm.dm_sales_by_salesperson AS
SELECT 
    sp.sales_name_id,
    sp.sales_name,
    sp.sales_age,
    sp.sales_gender,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_sales_name sp ON f.sales_name_id = sp.sales_name_id
GROUP BY sp.sales_name_id, sp.sales_name, sp.sales_age, sp.sales_gender;

--Membersihkan data lama dan mengisi ulang dengan data terbaru.
TRUNCATE TABLE dm.dm_sales_by_salesperson;

INSERT INTO dm.dm_sales_by_salesperson
SELECT 
    sp.sales_name_id,
    sp.sales_name,
    sp.sales_age,
    sp.sales_gender,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_sales_amount) AS total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_sales_name sp ON f.sales_name_id = sp.sales_name_id
GROUP BY sp.sales_name_id, sp.sales_name, sp.sales_age, sp.sales_gender;


--5. Tabel dm.dm_sales_summary
--Tabel ringkasan lengkap dari penjualan yang menggabungkan informasi toko, waktu, produk, dan penjual.
CREATE TABLE IF NOT EXISTS dm.dm_sales_summary AS
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
    f.total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_store s ON f.store_id = s.store_id
JOIN dwh.dim_time t ON f.time_id = t.time_id
JOIN dwh.dim_product p ON f.product_id = p.product_id
JOIN dwh.dim_sales_name sp ON f.sales_name_id = sp.sales_name_id;

--Membersihkan data lama dan mengisi ulang dengan data terbaru.
TRUNCATE TABLE dm.dm_sales_summary;

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
    f.total_sales_amount,
    CURRENT_TIMESTAMP AS created_at
FROM dwh.fact_sales_transaction f
JOIN dwh.dim_store s ON f.store_id = s.store_id
JOIN dwh.dim_time t ON f.time_id = t.time_id
JOIN dwh.dim_product p ON f.product_id = p.product_id
JOIN dwh.dim_sales_name sp ON f.sales_name_id = sp.sales_name_id;






























# ETL Assignment: Stored Procedure

## Objective
Design and implement a stored procedure, `dwh.generate_sales()`, to automate the ETL (Extract, Transform, Load) process.

## Scope
1. **Extract**: Transfer data from transactional database tables to staging tables.
2. **Transform**: Organize and load the data from staging tables into dimension tables in the data warehouse.
3. **Load**: Aggregate and load processed data into a fact table and data marts for analysis.

## Key Requirements
1. **SQL Knowledge**: Basic understanding of SQL and PostgreSQL database operations.
2. **Database Access**: Access to a PostgreSQL database to execute the queries and stored procedure.
3. **Documentation**: Provide clear in-line comments explaining each step in the stored procedure.

## Stored Procedure: `dwh.generate_sales()`
### Goals
- Automate the ETL process to move, transform, and load data efficiently from source tables to staging, data warehouse, and data marts.
- Ensure data integrity and consistency across schemas.

### ETL Process Steps
1. **Extract**:
   - Transfer data from the `public.sales_transaction` table to the staging table `stg.stg_sales_transaction`.
2. **Transform**:
   - Load data from `stg` into dimension tables (`dim_store`, `dim_product`, `dim_time`, `dim_sales_name`) in the `dwh` schema.
3. **Load**:
   - Insert new data into the fact table (`fact_sales_transaction`).
   - Populate aggregated data into data marts (`dm_sales_by_store`, `dm_sales_by_product`, `dm_sales_by_time`, `dm_sales_by_salesperson`, `dm_sales_summary`).

## Project Structure
```
ETL-Data-Warehouse-Generate-Sales/
├── SQL Query/
│   ├── 1.sales_transaction.sql  -- Create and populate the transactional database table.
│   ├── 2.stg.sql                -- Create and populate staging tables.
│   ├── 3.dwh-dim-fact.sql       -- Create and populate dimension and fact tables in the data warehouse.
│   ├── 4.dm.sql                 -- Create and populate data marts.
│   ├── 5.Store Procedure.sql    -- Full implementation of the stored procedure (generate_sales).
│   └── 6.update data.sql        -- Insert additional test data to observe ETL updates.
├── ETL Process with Stored Procedure.pdf -- Presentation explaining the ETL process.
├── README.md
```

## Steps to Run the Query
1. **Step 1: Create and Populate the Transactional Table**
   - Run `1.sales_transaction.sql` to create the `sales_transaction` table in the `public` schema and populate it with test data.

2. **Step 2: Create Staging Tables**
   - Run `2.stg.sql` to create the staging table (`stg_sales_transaction`) in the `stg` schema.

3. **Step 3: Create Data Warehouse Tables**
   - Run `3.dwh-dim-fact.sql` to create and populate:
     - Dimension tables (`dim_store`, `dim_product`, `dim_time`, `dim_sales_name`).
     - Fact table (`fact_sales_transaction`).

4. **Step 4: Create Data Marts**
   - Run `4.dm.sql` to create and populate:
     - Data mart tables (`dm_sales_by_store`, `dm_sales_by_product`, `dm_sales_by_time`, `dm_sales_by_salesperson`, `dm_sales_summary`).

5. **Step 5: Execute the Stored Procedure**
   - Run `5.Store Procedure.sql` to define the stored procedure `dwh.generate_sales()` and execute it to automate the entire ETL process.

6. **Step 6: Update and Test Data**
   - Run `6.update data.sql` to add new test data to `public.sales_transaction` and re-execute `generate_sales()` to validate updates across all schemas.

## Execution Example
To execute the stored procedure, use:
```sql
CALL dwh.generate_sales();
```

## Expected Outcomes
1. **Staging**:
   - The `stg_sales_transaction` table in the `stg` schema should be populated with the latest data from `public.sales_transaction`.

2. **Data Warehouse**:
   - Dimension tables (`dim_store`, `dim_product`, etc.) should reflect unique and up-to-date data.
   - The fact table (`fact_sales_transaction`) should contain all sales transactions.

3. **Data Marts**:
   - Aggregated data should be present in `dm_sales_by_store`, `dm_sales_by_product`, `dm_sales_by_time`, `dm_sales_by_salesperson`, and `dm_sales_summary`.

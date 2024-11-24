--Inserting new data into public.sales_transaction table
INSERT INTO public.sales_transaction(
		sale_id,store_id,store_name,city,state,country,sales_name_id,sales_name,
		sales_age,sales_gender,time_id,"date",day_of_week,"month","year",
		product_id,product_name,category,quantity,price) 
VALUES
	 (13,3,'Starbucks Corner','Chicago','IL','USA',11,'Hijir Della Wirasti',
	  31,'Female',7,'2024-01-07','Sunday','January',2024,
	  10,'Mocha','Coffee',4,6);
	  
--Calling the stored procedure to generate the datawarehouse
CALL dwh.generate_sales();


DELETE FROM public.sales_transaction
WHERE sales_name = 'Hijir Della Wirasti';
--this script creates silver layer tables after data cleansing and transformation from bronze layer
create or alter procedure silver.load_silver as
begin
	declare @batch_start_time datetime,@batch_end_time datetime;
	begin try
		set @batch_start_time=GETDATE()
		--this is 1st table crm
		truncate table silver.crm_cust_info;
		print '>> Insert data into: silver.crm cust info'
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status))='S' then 'Single'
			 when upper(trim(cst_gndr))='M' then 'Married'
			 else 'n/a'
		end cst_material_status,
		case when upper(trim(cst_gndr))='F' then 'Female'
			 when upper(trim(cst_gndr))='M' then 'Male'
			 else 'n/a'  -- normalize gender values
		end cst_gndr,
		cst_create_date
		from(select
		*,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info 
		where cst_id is not null)t  --this removes duplicates in tables and displays each  cst_id once(most recent records of cust)
		where flag_last=1

		--silver 2nd table
		truncate table silver.crm_prd_info
		print '>>insert data into silver crm prd info'
		insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select
		prd_id,
		replace(SUBSTRING(prd_key,1,5), '-','_')as cat_id, --extract category id
		substring(prd_key,7,len(prd_key)) as prd_key,--extract product key
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case when upper(trim(prd_line))='M' then 'Mountain'
			 when upper(trim(prd_line))='R' then 'Road'
			 when upper(trim(prd_line))='S' then 'Other Sales'
			 when upper(trim(prd_line))='T' then 'Touring'
			 else 'N/a'
		end prd_line, --map product line codes to descriptive values 

		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_start_dt order by prd_start_dt)-1 as date) as prd_end_dt --calculate end date as one day before the next start date
		from bronze.crm_prd_info

		--silver 3rd table

		truncate table silver.crm_sales_details
		print '>> insert data into silver crm sales details'
		insert into silver.crm_sales_details(
				sls_ord_num ,
				sls_prd_key ,
				sls_cust_id ,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price

		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt=0 or len(sls_order_dt) !=8 then null
			 else cast(cast(sls_order_dt as varchar)as date)
		end as sls_order_dt,--handling invalid dates
		case when sls_ship_dt=0 or len(sls_ship_dt) !=8 then null
			 else cast(cast(sls_ship_dt as varchar)as date)
		end as sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt) !=8 then null
			 else cast(cast(sls_due_dt as varchar)as date)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				 then sls_quantity*abs(sls_price)
			 else sls_sales--recalculate sales if original value is missing or incorrect
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price<=0
				 then sls_sales / nullif(sls_quantity,0)
			 else sls_price
		end as sls_price
		from bronze.crm_sales_details

		--4th table silver erp 1st

		truncate table silver.erp_CUST_AZ12
		print '>> insert data into silver.erp cust az12'
		INSERT INTO silver.erp_CUST_AZ12(
		cid,
		bdate,
		gen

		)
		select
		case when cid like 'NAS%' then substring(cid,4,len(cid))
			 else cid
		end cid,-- removed 'NAS'prefix if present
		CASE WHEN bdate > getdate() then null  
			 else bdate
		end as bdate,--set future birthdays to null
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			 when upper(trim(gen)) in ('M','MALE') then 'Male'
			 else 'n/a'--normalize gender values and unknown ones too
		end as gen
		from bronze.erp_CUST_AZ12

		--5th table erp2

		truncate table silver.erp_LOC_A101
		print '>> insert data into silver.erp loc a101'
		insert into silver.erp_LOC_A101(
		cid,
		cntry
		)
		select 
		REPLACE(cid,'-','')cid,
		case when trim(cntry)='DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			 else trim(cntry)
		end as cntry--normalize and handle missing or blank country codes
		from bronze.erp_LOC_A101

		--6th table erp3rd

		truncate table silver.erp_PX_CAT_G1V2
		print '>> insert data into silver erp px cat g1v2'
		INSERT INTO silver.erp_PX_CAT_G1V2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		select
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from bronze.erp_PX_CAT_G1V2

		print '===================================='
			set @batch_end_time=GETDATE()
			print 'Batch loading time:'+ cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar) +'seconds'
			print '===================================='
	end try
	begin catch
		print'============================'
		print'error' + error_message()
		print 'error' + cast(error_number() as nvarchar)
	end catch
end

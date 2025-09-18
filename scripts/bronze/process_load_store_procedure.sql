-- it loads store procedure: bronze  ( source --> bronze  )
--1.creates store procedure if doesnt exist and updates if exists on executing,has try-catch,calculate batch loadinf timr( basically time to load data from .csv files to database tables  
-- uses bulk insert to load data from .csv to tables
-- to use execute --> exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
begin
	declare @batch_start_time datetime,@batch_end_time datetime;
	begin try
		set @batch_start_time=GETDATE()
		print '=============================================================';
		print 'Loading Bronze Layer';
		print '=============================================================';
		truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from 'C:\DataWarehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock 
		);

		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\DataWarehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock 
		);

		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\DataWarehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock 
		);

		truncate table bronze.erp_CUST_AZ12
		bulk insert bronze.erp_CUST_AZ12
		from 'C:\DataWarehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock 
		);

		truncate table bronze.erp_LOC_A101
		bulk insert bronze.erp_LOC_A101
		from 'C:\DataWarehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock 
		);

		truncate table bronze.erp_PX_CAT_G1V2
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\DataWarehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock 
		);
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

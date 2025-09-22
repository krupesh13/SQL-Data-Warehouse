-- this script creates views for gold layer  (Star Schema is used )
-- this script can directly be cused for analytics and reporting
-- dimension customer
create view gold.dim_customers as
SELECT 
	row_number() over (order by  cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	ci.cst_material_status as marital_status,
	ci.cst_create_date as create_date,
	ca.bdate as birthdate,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is master src
	     else coalesce(ca.gen,'n/a')
	end as gender,
	la.cntry as country
FROM silver.crm_cust_info as ci
left join silver.erp_CUST_AZ12 as ca
on ci.cst_key = ca.cid
left join silver.erp_LOC_A101 as la
on ci.cst_key=la.cid
-- dimensions products
create view gold.dim_products as
select
ROW_NUMBER() over (order by pn.prd_start_dt , pn.prd_key)as product_key,
pn.prd_id as product_id,
pn.cat_id as category_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date,
pc.cat as categrory,
pc.subcat as subcategory,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
on pn.cat_id=pc.id
WHERE pn.prd_end_dt is null
-- Gold layer sales facts
create view gold.fact_sales as
select
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id

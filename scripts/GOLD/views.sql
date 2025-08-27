# this file consist of creation of consumer layer with views made by dimension table and fact tables  #

-----------------------dim_customer----------------------------------------------------------------



select * from crm_cust_info;
create or replace view gold.dim_customer as 
(select 
row_number() over(order by cst_id ) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as firstname,
ci.cst_lastname as lastname,
cl.cntry AS country,
ci.cst_marital_status AS marital_status,
case
 when ci.cst_gndr != 'O' then ci.cst_gndr
 else coalesce(ca.gen,'n/a') 
end as gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date,
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca on ci.cst_id = ca.cid
left join silver.erp_loc_a101 cl on ci.cst_id = cl.cid);
---------------------------------------------------------------------------------------------

select * from crm_prd_info;
select * from erp_x_cat_g1v2;

create or replace view gold.dim_products as 
select 
row_number() over(order by pi.prd_key, pi.prd_start_dt ) as product_key,
PI.PRD_ID as product_id,
PI.PRD_KEY as product_number,
PI.prd_nm as product_name,
CX.cat as category_id , 
CX.subcat as subcategory,
PI.prd_cost as product_cost,
PI.prd_line as production_line,
PI.prd_start_dt as production_start_date,
PI.prd_end_dt as production_end_date,
CX.maintenance
from silver.crm_prd_info PI
left join silver.erp_x_cat_g1v2 CX on PI.catg_id = CX.ID
where PI.prd_end_dt is null;

--------------------------------------------------------------FACT_SALES -----------------------------------------------------

select * from crm_sales_details;
select * from crm_cust_info;
select * from crm_prd_info;


CREATE OR REPLACE VIEW GOLD.FACT_SALES AS 
select 
CS.sls_ord_num,
DC.CUSTOMER_ID,
DP.product_number,
CS.sls_order_dt,
CS.sls_ship_dt,
CS.sls_due_dt,
CS.sls_sales,
CS.sls_quantity,
CS.sls_price
from silver.crm_sales_details CS
left join GOLD.DIM_CUSTOMER DC on CS.SLS_CUST_ID = DC.CUSTOMER_ID
left join GOLD.DIM_PRODUCTS DP on CS.SLS_PRD_KEY = DP.PRODUCT_NUMBER;



select * from GOLD.DIM_PRODUCTS;
select * from GOLD.DIM_CUSTOMER;
SELECT * FROM GOLD.FACT_SALES;

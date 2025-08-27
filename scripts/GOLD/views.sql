# this file consist of creation of consumer layer with views made by dimension table and fact tables  #

-----------------------------DIIM_CUSTOMER----------------------------------------

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

--------------------------------------------------------------------------------------

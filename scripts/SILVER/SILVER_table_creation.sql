--silver tables ddl :

create or replace table dwh.silver.crm_cust_info(
cst_id int,
cst_key varchar(30),
cst_firstname varchar(30),
cst_lastname varchar(30),
cst_marital_status varchar(1),
cst_gndr varchar(1),
cst_create_date date
);


create or replace table dwh.silver.crm_prd_info(
prd_id int,
catg_id varchar(10),
prd_key varchar(30),
prd_nm varchar(30),
prd_cost int,
prd_line varchar(2),
prd_start_dt date,
prd_end_dt date);


create or replace table dwh.silver.crm_sales_details(
sls_ord_num varchar(30)
,sls_prd_key varchar(30)
,sls_cust_id int
,sls_order_dt date
,sls_ship_dt date
,sls_due_dt date
,sls_sales int
,sls_quantity int
,sls_price int );

create or replace table dwh.silver.erp_cust_AZ12(
CID varchar(30),
BDATE date,
GEN varchar(1) );


create or replace table dwh.silver.erp_LOC_A101(
CID VARCHAR(30),
CNTRY VARCHAR(30));


create or replace table dwh.silver.erp_X_CAT_G1V2(
ID VARCHAR(10)
,CAT VARCHAR(30)
,SUBCAT VARCHAR(30)
,MAINTENANCE VARCHAR(3));

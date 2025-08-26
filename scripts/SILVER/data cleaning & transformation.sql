#  here are the data cleaning transformations and insert scripts from bronze to silver layer #



----------------------------------------------------TRANSFORMATION FOR DATA CLEANING  AND LOADING DATA IN SILVER STAGE ---------------
INSERT INTO DWH.SILVER.CRM_CUST_INFO(
CST_ID, CST_KEY,CST_CREATE_DATE,CST_MARITAL_STATUS,CST_FIRSTNAME,CST_LASTNAME , CST_GNDR
)
SELECT 
CST_ID,CST_KEY,CST_CREATE_DATE,cst_marital_status,
TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME,
TRIM(CST_LASTNAME) AS CST_LASTNAME,
TRIM(CST_GNDR) AS CST_GNDR,
FROM 
(SELECT * , 
ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC)
AS ID_RANK
FROM CRM_CUST_INFO
WHERE CST_ID IS NOT NULL)
WHERE ID_RANK =1 ;


--------------------------------------------------------------------------------CRM_PRD_INFO
INSERT INTO DWH.SILVER.CRM_PRD_INFO(
PRD_ID,PRD_NM,CATG_ID,PRD_KEY,PRD_COST,PRD_LINE,PRD_START_DT,PRD_END_DT)
SELECT 
PRD_ID,
PRD_NM,
REPLACE(SUBSTR(PRD_KEY,1,5),'-','_') AS CATG_ID,
SUBSTR(PRD_KEY,7) AS PRD_KEY,
(NVL2(PRD_COST, PRD_COST,0) )AS PRD_COST,
TRIM(NVL2(PRD_LINE,PRD_LINE,'NA')) AS PRD_LINE ,--NA = NOT AVAILABLE 
PRD_START_DT,
TRY_TO_DATE(
LEAD(PRD_START_DT) OVER (PARTITION BY PRD_KEY ORDER BY PRD_END_DT)-1
) AS PRD_END_DT
FROM CRM_PRD_INFO
;

--------------------------------------------------------------------------------crm_sales_details
insert into SILVER.CRM_SALES_DETAILS(
sls_ord_num,
sls_cust_id,
sls_prd_key,
sls_quantity,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_price
)
select 
sls_ord_num,
sls_cust_id,
sls_prd_key,
sls_quantity,
case
 when sls_order_dt = 0 or length(sls_order_dt) != 8 then null 
 else cast(cast(sls_order_dt as varchar) as date )
end as sls_order_dt ,
to_date(sls_ship_dt::text, 'YYYYMMDD') as sls_ship_dt,
to_date(sls_due_dt::text, 'YYYYMMDD') as sls_due_dt,
case
   when sls_sales<= 0 or sls_sales != sls_quantity * abs(sls_price)
   then sls_quantity * abs(sls_price )
   else sls_sales
end as sls_sales,
case 
   when sls_price <=0 or sls_price != sls_sales/ sls_quantity then sls_sales/sls_quantity
   else sls_price::FLOAT
end as sls_price,
from crm_sales_details;

# this file consist of code for ingesting data in our raw layer i.e bronze layer from our internal stage with the help of copy into command . #



---------------------------------------------------------------------------------------------------------------------------------------

select * from DWH.BRONZE.CRM_SALES_DETAILS;

-- to check if columns of the certain table are present in our internal stage or is table has data in our internal stage ----
SELECT $1,$2,$3 FRoM @STAGE_AZURE/cust_info.csv;


--- copy inot command for loading data from internal stage to our bronze layer -----
copy into DWH.BRONZE.CRM_CUST_INFO
FROM @STAGE_AZURE/cust_info.csv;


--- to list all the files present in our internal stage ----
list  @STAGE_AZURE;

---------------------------------------------------------
copy into DWH.BRONZE.ERP_PX_CAT_G1V2
FROM @STAGE_AZURE/PX_CAT_G1V2.csv;

--------------------------------------------------

#This script consist of our first few project enviornment setup as keeping azure blob storage for our data container and then connecting to snowflake through internal stage. 
second step is to create the databse as well as our schemas #










-- CREATING THE AZURE_INTEGRATION---

CREATE or replace STORAGE INTEGRATION  AZURE_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '23c4ca8c-d226-454f-80a9-45adc19d9eac'
  STORAGE_ALLOWED_LOCATIONS = ('azure://storagedwhproject.blob.core.windows.net/dwhstorage');

--- CHECK FOR STORAGE INTEGRATION ---
DESC STORAGE INTEGRATION AZURE_INTEGRATION;

  -- FILE FORMAT CREAATION ---

  CREATE OR REPLACE FILE FORMAT DWH.PUBLIC.FILE_FORMAT 
   TYPE = CSV
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1 
        NULL_IF = ('', 'NULL'); 



  ----CREATE INTERNAL STAGE ----
  CREATE OR REPLACE STAGE DWH.PUBLIC.STAGE_AZURE
  STORAGE_INTEGRATION = AZURE_INTEGRATION
  URL = 'azure://storagedwhproject.blob.core.windows.net/dwhstorage'
  FILE_FORMAT= FILE_FORMAT;


  ---- LIST OF FILES IN OUR STORAGE---

  LIST @DWH.PUBLIC.STAGE_AZURE;
  
  
  -------------------------------------------------------------------------------------------------------------------------
  
  
  -- create databse command--
create database dwh;

-- use the database --
use database dwh;

---creating the schemas --
create schema bronze;
create schema silver;
create schema gold;

----------------------------------------------------------------------------------------------------------------------------------------


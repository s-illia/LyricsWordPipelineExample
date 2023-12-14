-- ----------------------------------------------------------------------------
-- Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- Step #2: Setup Logging
-- ----------------------------------------------------------------------------
--Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE R_SERVICE;
GRANT ROLE R_SERVICE TO ROLE SYSADMIN;
GRANT ROLE R_SERVICE TO USER IDENTIFIER($MY_USER);
-- Database
CREATE OR REPLACE DATABASE DB_SERVICE;
GRANT OWNERSHIP ON DATABASE DB_SERVICE TO ROLE R_SERVICE;

-- SCHEMA
USE ROLE R_SERVICE;
USE DATABASE DB_SERVICE;

CREATE OR REPLACE SCHEMA SERVICE;
--Event Table
CREATE EVENT TABLE DB_SERVICE.SERVICE.LOG_EVENTS;
--Configure account
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET EVENT_TABLE = DB_SERVICE.SERVICE.LOG_EVENTS;


-- ----------------------------------------------------------------------------
-- Step #2: Setup Storage integration
-- https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
-- UPADTE ACCOUNT NAME
-- ----------------------------------------------------------------------------
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::1122334455667788:role/snowflake_role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://lyrics-word-raw-bucket/csv/');

--get parameters to activate integration
DESC INTEGRATION s3_integration;
--STORAGE_AWS_IAM_USER_ARN
--STORAGE_AWS_EXTERNAL_ID
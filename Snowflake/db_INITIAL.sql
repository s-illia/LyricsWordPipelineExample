-- ----------------------------------------------------------------------------
-- Step #3: Setup DB_LYRICS_ANALYSIS
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE R_LYRICS_ANALYSIS;
GRANT ROLE R_LYRICS_ANALYSIS TO ROLE SYSADMIN;
GRANT ROLE R_LYRICS_ANALYSIS TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE R_LYRICS_ANALYSIS;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE R_LYRICS_ANALYSIS;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE R_LYRICS_ANALYSIS;
GRANT USAGE ON INTEGRATION s3_integration TO ROLE R_LYRICS_ANALYSIS;
GRANT USAGE ON DATABASE DB_SERVICE TO ROLE R_LYRICS_ANALYSIS;
GRANT USAGE ON SCHEMA DB_SERVICE.SERVICE TO ROLE R_LYRICS_ANALYSIS;
GRANT SELECT ON TABLE DB_SERVICE.SERVICE.LOG_EVENTS TO ROLE R_LYRICS_ANALYSIS;


-- Database
CREATE OR REPLACE DATABASE DB_LYRICS_ANALYSIS;
GRANT OWNERSHIP ON DATABASE DB_LYRICS_ANALYSIS TO ROLE R_LYRICS_ANALYSIS;
ALTER DATABASE DB_LYRICS_ANALYSIS SET LOG_LEVEL = TRACE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE WH_LYRICS_ANALYSIS WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE WH_LYRICS_ANALYSIS TO ROLE R_LYRICS_ANALYSIS;

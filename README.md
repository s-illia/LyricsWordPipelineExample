# Lyrics Word Data Flow Example
A small practice project to build a ETL data ingestion pipline, integrating AWS and Snowflake.
The initial idea is to build fully automated pipeline that will update data in DWH when source datasets are updated. There should be no servers to mannage and no manual actions to trigger.

As a theme I chose a dataset which contains the number of occurances of words inside of song lyrics. The Lyrics dataset is enriched with song title, artist name, and song genre from other datasets. The final database can be used for analysys of lyrics inside songs accross different dimensions.

Here is an example of Tableau dashboard, built with data from the final database:
- Average accurance of words 'Friend' and 'Enemy' in songs in each genre (Folk is the friendliest - though, we are not analyzing the sentiment and it's just a word accurance).
- Most popular words across all genres (Who could have thought that they will be mostly pronouns and prepositions?).
![Dashboard](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/TABLEAU_DASHBOARD.png?raw=true)

## Workflow
![Workflow](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/diagram.png?raw=true)
### AWS 
Required datasets are downloaded using AWS Lambda. Python function is using SSM Parameter Store to get the configuration - URLs, formats, schema. Simple transformations are made - only required columns are extracted where possible, datasets are converted into unified CSV format. The CSV files are stored on an S3 bucket. Lambda is scheduled in EventBridge to constantly check for updates.

### Snowflake
Major transformations are made in Snowflake. I use Streams to capture data changes. Code for data transformation and load is organized in Stored Procedures. Tasks are used for orchestration.
I use 3 schemas to store and process data.

#### Staging
![Staging](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/SCHEMA_RAW_DATA.png?raw=true)
Schema `RAW_DATA` is used for staging. External stge is linked with AWS using Integration object. Initial data from S3 is imported into `SRC_*` tables by stored procedure `SP_UPLOAD_SRC_TABLES`. The tables store raw data and are truncated before every load. 
Stored procedure `SP_UPSERT_STG_TABLES` takes the raw data from SRC_* tables and upserts it into STG_* tables. STG_* tables have correct data types and are SCDs type 2.

#### Normalized DB
![Normalized](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/SCHEMA_NORMALIZED.png?raw=true)
Schema `NORMALIZED` stores the data in normalized form in tables named `T_*`. Stored procedure `SP_UPSERT_T_TABLES` is used to populate the tables consuming corresponding streams `STR_STG_*`. The procedure makes grouping by natural keys. Synthetic keys are assigned by sequences. Kes integrity is also enforced with foreign keys. It also stores a view `VW_LYRICS` for more convinient change data capture for the next stage of transformations.

#### Denormalized WH
![Denormalized](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/SCHEMA_ANALYTICS.png?raw=true)
Schema `ANALYTICS` contains denormalized data in a star schema. Dimensions are updated by `SP_UPSERT_DIM_TABLES` from `STR_T_*` streams and a fact table is calculated by `SP_CALC_FCT_TABLES` from `STR_VW_LYRICS` stream. As the fact table contains all the data, even if genre is unresolved, view `VW_LYRICS_WORD_COUNTS` is created to get only the data we are interested in.

#### Orchestration
ETL orchestration inside Snowflake is made using tasks. Task `REFRESH_ALL_TABLES` is the head - launching it starts the process. Stored procedures are launched only if there are data in the streams.

#### Logging
Event Table `DB_SERVICE.SERVICE.LOG_EVENTS` in separate DB is used for logging all activities of stored procedures.

## Installation
### AWS
You should have Terraform and configured AWS CLI.
Edit `AWS/terraform.tfvars.json` (your Region and S3 Bucket name). 
Install AWS objects using Terraform template `main.tf` in `AWS` folder.
The template will create a S3 Bucket, SSM Parameters, a Lambda function and a *disabled* EventBridge schedule, as well all required roles and permissions.
If you need, setup [AWS integration][https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration] with Snowflake.

### Snowflake
Configure s3_integration - use your Account Name and S3 Bucket.
Go to `Snowflake` folder. Use console to ceate DB objects.
Installation order is as follows:
 - `ACCOUNT_DDL.sql` - account level objects - Event Table and AWS Storage integration
 - `DB_INITIAL.sql` - initializing DB - Roles, Databases and Warehouses
 - `DB_LYRICS_ANALYSIS_DDL.sql` - DB level objects - Tables, Stages, Views, etc.
 - Install stored procedures:
    - `SP_CALC_FCT_TABLES.sql`
    - `SP_UPLOAD_SRC_TABLES.sql`
    - `SP_UPSERT_DIM_TABLES.sql`
    - `SP_UPSERT_STG_TABLES.sql`
    - `SP_UPSERT_T_TABLES.sql`
 - `TASKS_DDL.sql` - Tasks for ETL Orchestration

## Datasets
### Lyrycs, song and artist names, unstammed words
http://millionsongdataset.com/musixmatch/
### Genres
https://www.tagtraum.com/msd_genre_datasets.html

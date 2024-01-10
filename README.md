# Lyrics Word Data Flow Example

A small practice project to build an ETL data ingestion pipeline, integrating AWS and Snowflake. The initial idea is to create a fully automated pipeline that updates data in the Data Warehouse (DWH) when source datasets are updated. The goal is to have no servers to manage and no manual actions to trigger.

As a theme, I chose a dataset containing the number of occurrences of words inside song lyrics. The Lyrics dataset is enriched with song titles, artist names, and song genres from other datasets. The final database can be used for the analysis of lyrics within songs across different dimensions.

Here is an example Tableau dashboard built with data from the final database:
- Average occurrence of words 'Friend' and 'Enemy' in songs in each genre (Folk is the friendliest - though, we are not analyzing the sentiment and it's just a word occurrence).
- Most popular words across all genres (Who could have thought that they will be mostly pronouns and prepositions?).

![Dashboard](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/TABLEAU_DASHBOARD.png?raw=true)

## Workflow

![Workflow](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/diagram.png?raw=true)

### AWS

Required datasets are downloaded using AWS Lambda. A Python function uses SSM Parameter Store to get the configuration - URLs, formats, schema. Simple transformations are made; only required columns are extracted where possible, datasets are converted into a unified CSV format. The CSV files are stored in an S3 bucket. Lambda is scheduled in EventBridge to constantly check for updates.

### Snowflake

Major transformations are made in Snowflake. I use Streams to capture data changes. Code for data transformation and load is organized in Stored Procedures. Tasks are used for orchestration. I use three schemas to store and process data.

#### Staging

![Staging](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/SCHEMA_RAW_DATA.png?raw=true)

Schema `RAW_DATA` is used for staging. External stage is linked with AWS using Integration object. Initial data from S3 is imported into `SRC_*` tables by stored procedure `SP_UPLOAD_SRC_TABLES`. The tables store raw data and are truncated before every load. Stored procedure `SP_UPSERT_STG_TABLES` takes the raw data from SRC_* tables and upserts it into STG_* tables. STG_* tables have correct data types and are SCDs type 2.

#### Normalized DB

![Normalized](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/SCHEMA_NORMALIZED.png?raw=true)

Schema `NORMALIZED` stores the data in normalized form in tables named `T_*`. Stored procedure `SP_UPSERT_T_TABLES` is used to populate the tables consuming corresponding streams `STR_STG_*`. The procedure makes grouping by natural keys. Synthetic keys are assigned by sequences. Key integrity is also enforced with foreign keys. It also stores a view `VW_LYRICS` for more convenient change data capture for the next stage of transformations.

#### Denormalized WH

![Denormalized](https://github.com/s-illia/LyricsWordPipelineExample/blob/main/SCHEMA_ANALYTICS.png?raw=true)

Schema `ANALYTICS` contains denormalized data in a star schema. Dimensions are updated by `SP_UPSERT_DIM_TABLES` from `STR_T_*` streams, and a fact table is calculated by `SP_CALC_FCT_TABLES` from `STR_VW_LYRICS` stream. As the fact table contains all the data, even if the genre is unresolved, view `VW_LYRICS_WORD_COUNTS` is created to get only the data we are interested in.

#### Orchestration

ETL orchestration inside Snowflake is made using tasks. Task `REFRESH_ALL_TABLES` is the head - launching it starts the process. Stored procedures are launched only if there is data in the streams.

#### Logging

Event Table `DB_SERVICE.SERVICE.LOG_EVENTS` in a separate DB is used for logging all activities of stored procedures.

## Installation

### AWS

You should have Terraform and configured AWS CLI. Edit `AWS/terraform.tfvars.json` (your Region and S3 Bucket name). Install AWS objects using Terraform template `main.tf` in the `AWS` folder. The template will create an S3 Bucket, SSM Parameters, a Lambda function, and a *disabled* EventBridge schedule, as well all required roles and permissions. If you need, set up [AWS integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) with Snowflake.

### Snowflake

Configure `s3_integration` - use your Account Name and S3 Bucket. Go to the `Snowflake` folder. Use the console to create DB objects. Installation order is as follows:
- Create objects:
   - `ACCOUNT_DDL.sql` - account-level objects - Event Table and AWS Storage integration
   - `DB_INITIAL.sql` - initializing DB - Roles, Databases, and Warehouses
   - `DB_LYRICS_ANALYSIS_DDL.sql` - DB level objects - Tables, Stages, Views, etc.
- Install stored procedures:
   - `SP_CALC_FCT_TABLES.sql`
   - `SP_UPLOAD_SRC_TABLES.sql`
   - `SP_UPSERT_DIM_TABLES.sql`
   - `SP_UPSERT_STG_TABLES.sql`
   - `SP_UPSERT_T_TABLES.sql`
- Create Tasks
   - `TASKS_DDL.sql` - Tasks for ETL Orchestration

## Datasets

### Lyrics, song and artist names, unstemmed words

[Million Song Dataset - Musixmatch](http://millionsongdataset.com/musixmatch/)

### Genres

[Tagtraum Industries MSD Genre Datasets](https://www.tagtraum.com/msd_genre_datasets.html)

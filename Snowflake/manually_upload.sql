USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
USE SCHEMA NORMALIZED;
SHOW streams;
CALL RAW_DATA.SP_UPSERT_STG_TABLES();
CALL NORMALIZED.SP_UPSERT_T_TABLES();
CALL ANALYTICS.SP_UPSERT_DIM_TABLES();
CALL ANALYTICS.SP_CALC_FCT_TABLES();

select TIMESTAMP, VALUE from DB_SERVICE.SERVICE.LOG_EVENTS order by TIMESTAMP DESC;

select count(*) from ANALYTICS.DIM_SONGS;

select count(*) from ANALYTICS.FCT_LYRICS_WORD_COUNTS;
--7004392
--19045277
select * from FCT_LYRICS_WORD_COUNTS limit 100;
select count(*) from ANALYTICS.FCT_LYRICS_WORD_COUNTS where artist_id  = -1;
select count(*) from ANALYTICS.FCT_LYRICS_WORD_COUNTS where word_id  = -1;
select count(*) from ANALYTICS.FCT_LYRICS_WORD_COUNTS where genre_id  = -1;
select count(*) from ANALYTICS.FCT_LYRICS_WORD_COUNTS where word_count  <= 0;
select * from RAW_DATA.STG_SONGS limit 100;
select count(DISTINCT(artist_code)) from NORMALIZED.T_ARTISTS limit 100;
--52310
select count(*) from NORMALIZED.T_ARTISTS limit 100;
--52310
select * from NORMALIZED.T_ARTISTS 
where artist_code in (select artist_code from NORMALIZED.T_ARTISTS group by artist_code having count(*) > 1);

select * from NORMALIZED.T_SONGS
where song_code in (select song_code from NORMALIZED.T_SONGS group by song_code having count(*) > 1)
 limit 100;

select * from RAW_DATA.STG_GENRES limit 100;

        SELECT
            STG_SONGS.MSD_TRACK_ID AS SONG_CODE,
            nvl(T_ARTISTS.ARTIST_ID, -1) as ARTIST_ID,
            nvl(T_GENRES.GENRE_ID, -1) as GENRE_ID,
            STG_SONGS.MSD_TITLE AS SONG_TITLE
        FROM
            RAW_DATA.STG_SONGS
            LEFT JOIN T_ARTISTS ON UPPER(STG_SONGS.MSD_ARTIST_NAME) = T_ARTISTS.ARTIST_CODE
			LEFT JOIN RAW_DATA.STG_GENRES ON STG_SONGS.MSD_TRACK_ID = STG_GENRES.TRACK_ID
            LEFT JOIN T_GENRES ON UPPER(STG_GENRES.GENRE) = T_GENRES.GENRE_CODE
        limit 100
;


USE SCHEMA RAW_DATA;
truncate table STG_SONGS;
truncate table STG_GENRES;
truncate table STG_WORDS;
truncate table STG_LYRICS;
USE SCHEMA NORMALIZED;
truncate table T_SONGS;
truncate table T_GENRES;
truncate table T_ARTISTS;
truncate table T_WORDS;
truncate table T_LYRICS;
truncate table DIM_SONGS;
truncate table DIM_GENRES;
truncate table DIM_ARTISTS;
truncate table DIM_WORDS;


USE SCHEMA ANALYTICS;
    -- Upsert data from T_SONGS to DIM_SONGS
    MERGE INTO DIM_SONGS AS target
    USING NORMALIZED.T_SONGS AS source
    ON target.SONG_ID = source.SONG_ID
    WHEN MATCHED THEN
        UPDATE SET
            SONG_CODE = source.SONG_CODE,
            SONG_TITLE = source.SONG_TITLE
    WHEN NOT MATCHED THEN
        INSERT (
            SONG_ID,
            SONG_CODE,
            SONG_TITLE
        )
        VALUES (
            source.SONG_ID,
            source.SONG_CODE,
            source.SONG_TITLE
        );

    -- Upsert data from T_GENRES to DIM_GENRES
    MERGE INTO DIM_GENRES AS target
    USING NORMALIZED.T_GENRES AS source
    ON target.GENRE_ID = source.GENRE_ID
    WHEN MATCHED THEN
        UPDATE SET
            GENRE_CODE = source.GENRE_CODE,
            GENRE_NAME = source.GENRE_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            GENRE_ID,
            GENRE_CODE,
            GENRE_NAME
        )
        VALUES (
            source.GENRE_ID,
            source.GENRE_CODE,
            source.GENRE_NAME
        );

    -- Upsert data from T_ARTISTS to DIM_ARTISTS
    MERGE INTO DIM_ARTISTS AS target
    USING (
        SELECT
            ARTIST_ID,
            ARTIST_CODE,
            ARTIST_NAME
        FROM
            NORMALIZED.T_ARTISTS
    ) AS source
    ON target.ARTIST_ID = source.ARTIST_ID
    WHEN MATCHED THEN
        UPDATE SET
            ARTIST_CODE = source.ARTIST_CODE,
            ARTIST_NAME = source.ARTIST_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            ARTIST_ID,
            ARTIST_CODE,
            ARTIST_NAME
        )
        VALUES (
            source.ARTIST_ID,
            source.ARTIST_CODE,
            source.ARTIST_NAME
        );

    -- Upsert data from T_WORDS to DIM_WORDS
    MERGE INTO DIM_WORDS AS target
    USING NORMALIZED.T_WORDS AS source
    ON target.WORD_ID = source.WORD_ID
    WHEN MATCHED THEN
        UPDATE SET
            WORD_CODE = source.WORD_CODE,
            WORD_NAME = source.WORD_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            WORD_ID,
            WORD_CODE,
            WORD_NAME
        )
        VALUES (
            source.WORD_ID,
            source.WORD_CODE,
            source.WORD_NAME
        );

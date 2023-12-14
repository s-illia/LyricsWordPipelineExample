USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
USE SCHEMA ANALYTICS;
CREATE OR REPLACE PROCEDURE SP_UPSERT_DIM_TABLES()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    -- Log the start of the procedure
    SYSTEM$LOG_INFO('Started');

    USE SCHEMA ANALYTICS;

    -- Upsert data from T_SONGS to DIM_SONGS
    MERGE INTO DIM_SONGS AS target
    USING STR_T_SONGS AS source
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

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for DIM_SONGS: ' || SQLROWCOUNT);

    -- Upsert data from T_GENRES to DIM_GENRES
    MERGE INTO DIM_GENRES AS target
    USING STR_T_GENRES AS source
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

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for DIM_GENRES: ' || SQLROWCOUNT);

    -- Upsert data from T_ARTISTS to DIM_ARTISTS
    MERGE INTO DIM_ARTISTS AS target
    USING (
        SELECT
            ARTIST_ID,
            ARTIST_CODE,
            ARTIST_NAME
        FROM
            STR_T_ARTISTS
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

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for DIM_ARTISTS: ' || SQLROWCOUNT);

    -- Upsert data from T_WORDS to DIM_WORDS
    MERGE INTO DIM_WORDS AS target
    USING STR_T_WORDS AS source
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

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for DIM_WORDS: ' || SQLROWCOUNT);
	
    -- Log the end of the procedure
    SYSTEM$LOG_INFO('Procedure completed successfully');
	
    -- Return a summary of the upsert operation
    RETURN 'Procedure executed successfully.';

EXCEPTION
    WHEN OTHER THEN
        BEGIN
            -- Log an error if an exception occurs
            SYSTEM$LOG_ERROR('SQLCODE ' || SQLCODE || ', SQLERRM ' || SQLERRM || ', SQLSTATE ' || SQLSTATE);
            ROLLBACK;
            RAISE;
        END;    
END;
$$;

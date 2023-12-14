USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
USE SCHEMA NORMALIZED;
CREATE OR REPLACE PROCEDURE SP_UPSERT_T_TABLES()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
	V_ND_ID BIGINT DEFAULT -1;
BEGIN
	-- Log the start of the procedure
    SYSTEM$LOG_INFO('Started');
    -- using same stream twice - need to wrap in a transaction
    BEGIN TRANSACTION;
	
    USE SCHEMA NORMALIZED;

    -- Upsert data from STG_WORDS to T_WORDS
    MERGE INTO T_WORDS AS target
    USING (
        SELECT
            WORD_CODE,
            min(WORD_NAME) as WORD_NAME
        FROM
            STR_STG_WORDS
        group by 
            WORD_CODE    
    ) AS source
    ON target.WORD_CODE = source.WORD_CODE
    WHEN MATCHED THEN
        UPDATE SET
            WORD_NAME = source.WORD_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            WORD_CODE,
            WORD_NAME
        )
        VALUES (
            source.WORD_CODE,
            source.WORD_NAME
        );

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for T_WORDS: ' || SQLROWCOUNT);
	
    -- Upsert data from STG_ARTISTS to T_ARTISTS
    MERGE INTO T_ARTISTS AS target
    USING (
        SELECT 
            UPPER(MSD_ARTIST_NAME) as ARTIST_CODE,
            min(MSD_ARTIST_NAME) as ARTIST_NAME
        FROM
            STR_STG_SONGS
        GROUP BY 
            UPPER(MSD_ARTIST_NAME)     
    ) AS source
    ON target.ARTIST_CODE = source.ARTIST_CODE
    WHEN MATCHED THEN
        UPDATE SET
            ARTIST_NAME = source.ARTIST_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            ARTIST_CODE,
            ARTIST_NAME
        )
        VALUES (
            source.ARTIST_CODE,
            source.ARTIST_NAME
        );

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for T_ARTISTS: ' || SQLROWCOUNT);
	
	-- Upsert data from STG_GENRES to T_GENRES
    MERGE INTO T_GENRES AS target
    USING (
        SELECT
            UPPER(GENRE) as GENRE_CODE,
            min(GENRE) as GENRE_NAME
        FROM
            STR_STG_GENRES 
        GROUP BY 
            UPPER(GENRE)
    ) AS source
    ON target.GENRE_CODE = source.GENRE_CODE
    WHEN MATCHED THEN
        UPDATE SET
            GENRE_NAME = source.GENRE_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            GENRE_CODE,
            GENRE_NAME
        )
        VALUES (
            source.GENRE_CODE,
            source.GENRE_NAME
        );

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for T_GENRES: ' || SQLROWCOUNT);
	
	-- Upsert data from STG_SONGS to T_SONGS
    MERGE INTO T_SONGS AS target
    USING (
        SELECT DISTINCT
            STG_SONGS.MSD_TRACK_ID AS SONG_CODE,
            nvl(T_ARTISTS.ARTIST_ID, :V_ND_ID) as ARTIST_ID,
            nvl(T_GENRES.GENRE_ID, :V_ND_ID) as GENRE_ID,
            STG_SONGS.MSD_TITLE AS SONG_TITLE
        FROM
            STR_STG_SONGS STG_SONGS
            LEFT JOIN T_ARTISTS ON UPPER(STG_SONGS.MSD_ARTIST_NAME) = T_ARTISTS.ARTIST_CODE
			LEFT JOIN RAW_DATA.STG_GENRES ON STG_SONGS.MSD_TRACK_ID = STG_GENRES.TRACK_ID
            LEFT JOIN T_GENRES ON UPPER(STG_GENRES.GENRE) = T_GENRES.GENRE_CODE
			
    ) AS source
    ON target.SONG_CODE = source.SONG_CODE
    WHEN MATCHED THEN
        UPDATE SET
            ARTIST_ID = source.ARTIST_ID,
            GENRE_ID = source.GENRE_ID,
            SONG_TITLE = source.SONG_TITLE
    WHEN NOT MATCHED THEN
        INSERT (
            ARTIST_ID,
            GENRE_ID,
            SONG_CODE,
            SONG_TITLE
        )
        VALUES (
            source.ARTIST_ID,
            source.GENRE_ID,
            source.SONG_CODE,
            source.SONG_TITLE
        );

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for T_SONGS: ' || SQLROWCOUNT);

    -- Upsert data from STG_LYRICS to T_LYRICS
    MERGE INTO T_LYRICS AS target
    USING (
        SELECT DISTINCT
            T_SONGS.SONG_ID,
            T_WORDS.WORD_ID,
            STG_LYRICS.COUNT
        FROM
            STR_STG_LYRICS STG_LYRICS
            JOIN T_SONGS ON STG_LYRICS.TRACK_ID = T_SONGS.SONG_CODE
            JOIN T_WORDS ON STG_LYRICS.WORD = T_WORDS.WORD_CODE
    ) AS source
    ON target.SONG_ID = source.SONG_ID AND target.WORD_ID = source.WORD_ID
    WHEN MATCHED THEN
        UPDATE SET
            COUNT = source.COUNT
    WHEN NOT MATCHED THEN
        INSERT (
            SONG_ID,
            WORD_ID,
            COUNT
        )
        VALUES (
            source.SONG_ID,
            source.WORD_ID,
            source.COUNT
        );

    -- Log the number of rows affected
    SYSTEM$LOG_INFO('Rows affected for T_LYRICS: ' || SQLROWCOUNT);
	
    -- Log the end of the procedure
    SYSTEM$LOG_INFO('Procedure completed successfully');
	
    -- Return a summary of the upsert operation
    COMMIT;
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

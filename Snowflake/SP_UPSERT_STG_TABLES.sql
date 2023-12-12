
USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
USE SCHEMA RAW_DATA;
CREATE OR REPLACE PROCEDURE SP_UPSERT_STG_TABLES()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
	V_SYSTEM_USER VARCHAR(30) DEFAULT 'System';
BEGIN
    -- Log the start of the procedure
    SYSTEM$LOG_INFO('Started');

    USE SCHEMA RAW_DATA;

    -- Update STG_SONGS from SRC_SONGS
    MERGE INTO STG_SONGS AS target
    USING SRC_SONGS AS source
    ON target.MSD_TRACK_ID = source.MSD_TRACK_ID
       AND target.MSD_ARTIST_NAME = source.MSD_ARTIST_NAME
       AND target.MSD_TITLE = source.MSD_TITLE
       AND target.MXM_TRACK_ID = source.MXM_TRACK_ID
       AND target.MXM_ARTIST_NAME = source.MXM_ARTIST_NAME
       AND target.MXM_TITLE = source.MXM_TITLE
    WHEN NOT MATCHED THEN
        INSERT (
            MSD_TRACK_ID,
            MSD_ARTIST_NAME,
            MSD_TITLE,
            MXM_TRACK_ID,
            MXM_ARTIST_NAME,
            MXM_TITLE,
            INSERT_DT,
            INSERT_BY
        )
        VALUES (
            source.MSD_TRACK_ID,
            source.MSD_ARTIST_NAME,
            source.MSD_TITLE,
            source.MXM_TRACK_ID,
            source.MXM_ARTIST_NAME,
            source.MXM_TITLE,
            CURRENT_TIMESTAMP(),
            :V_SYSTEM_USER
        );

    -- Log the number of affected rows for STG_SONGS
    SYSTEM$LOG_INFO('Rows affected for STG_SONGS: ' || SQLROWCOUNT);

    -- Similar logging for other tables...

    -- Update STG_LYRICS from SRC_LYRICS
    MERGE INTO STG_LYRICS AS target
    USING SRC_LYRICS AS source
    ON target.TRACK_ID = source.TRACK_ID
       AND target.WORD = source.WORD
       AND target."COUNT" = source."COUNT"::BIGINT
    WHEN NOT MATCHED THEN
        INSERT (
            TRACK_ID,
            WORD,
            COUNT,
            INSERT_DT,
            INSERT_BY
        )
        VALUES (
            source.TRACK_ID,
            source.WORD,
            source."COUNT"::bigint,
            CURRENT_TIMESTAMP(),
            :V_SYSTEM_USER
        );

    -- Log the number of affected rows for STG_LYRICS
    SYSTEM$LOG_INFO('Rows affected for STG_LYRICS: ' || SQLROWCOUNT);

-- Update STG_WORDS from SRC_WORDS
    MERGE INTO STG_WORDS AS target
    USING SRC_WORDS AS source
    ON target.WORD_CODE = source.STEMMED
       AND target.WORD_NAME = source.UNSTAMMED
    WHEN NOT MATCHED THEN
        INSERT (
            WORD_CODE,
            WORD_NAME,
            INSERT_DT,
            INSERT_BY
        )
        VALUES (
            source.STEMMED,
            source.UNSTAMMED,
            CURRENT_TIMESTAMP,
            :V_SYSTEM_USER
        );

    -- Log the number of affected rows for STG_WORDS
    SYSTEM$LOG_INFO('Rows affected for STG_WORDS: ' || SQLROWCOUNT);
	
    -- Update STG_GENRES from SRC_GENRES
    MERGE INTO STG_GENRES AS target
    USING SRC_GENRES AS source
    ON target.TRACK_ID = source.TRACK_ID
       AND target.GENRE = source.GENRE
    WHEN NOT MATCHED THEN
        INSERT (
            TRACK_ID,
            GENRE,
            INSERT_DT,
            INSERT_BY
        )
        VALUES (
            source.TRACK_ID,
            source.GENRE,
            CURRENT_TIMESTAMP,
            :V_SYSTEM_USER
        );
		
    -- Log the number of affected rows for STG_GENRES
    SYSTEM$LOG_INFO('Rows affected for STG_GENRES: ' || SQLROWCOUNT);
	
    -- Log the end of the procedure
    SYSTEM$LOG_INFO('Procedure completed successfully');

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

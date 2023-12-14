USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
USE SCHEMA ANALYTICS;
CREATE OR REPLACE PROCEDURE SP_CALC_FCT_TABLES()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    -- Log the start of the procedure
	SYSTEM$LOG_INFO('Started');

    USE SCHEMA ANALYTICS;
	
    -- Create a temporary table to store changed rows
    CREATE OR REPLACE TEMPORARY TABLE TMP_CHANGED_ROWS AS
    SELECT
        SONG_ID,
        ARTIST_ID,
        GENRE_ID,
        WORD_ID,
        SUM(CHANGED_LYRICS.COUNT) AS WORD_COUNT
    FROM
        STR_VW_LYRICS CHANGED_LYRICS
    GROUP BY
        SONG_ID,
        ARTIST_ID,
        GENRE_ID,
        WORD_ID;

    -- Log the number of changed rows
    SYSTEM$LOG_INFO('Changed rows calculated: ' || SQLROWCOUNT);

    -- Delete changed rows in the fact table
    DELETE FROM FCT_LYRICS_WORD_COUNTS
    WHERE (SONG_ID, ARTIST_ID, GENRE_ID, WORD_ID) IN (
        SELECT
            SONG_ID,
            ARTIST_ID,
            GENRE_ID,
            WORD_ID
        FROM
            TMP_CHANGED_ROWS
    );

    -- Log the number of deleted rows
    SYSTEM$LOG_INFO('Rows deleted for FCT_LYRICS_WORD_COUNTS: ' || SQLROWCOUNT);

    -- Insert new rows into the fact table
    INSERT INTO FCT_LYRICS_WORD_COUNTS (SONG_ID, ARTIST_ID, GENRE_ID, WORD_ID, WORD_COUNT)
    SELECT
        SONG_ID,
        ARTIST_ID,
        GENRE_ID,
        WORD_ID,
        WORD_COUNT
    FROM
        TMP_CHANGED_ROWS;

    -- Log the number of inserted rows
    SYSTEM$LOG_INFO('Rows inserted for FCT_LYRICS_WORD_COUNTS: ' || SQLROWCOUNT);

    -- Drop the temporary table
    DROP TABLE IF EXISTS TMP_CHANGED_ROWS;

	-- Log the end of the procedure
    SYSTEM$LOG_INFO('Procedure completed successfully');
	
    -- Return a summary of the calculation and insertion
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

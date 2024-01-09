
USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
USE SCHEMA RAW_DATA;
CREATE OR REPLACE PROCEDURE SP_UPLOAD_SRC_TABLES()
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

    truncate table SRC_LYRICS;
    COPY INTO SRC_LYRICS
    FROM @word_counts_stage_external/lyrycs.db.csv
    FORCE = TRUE;

    truncate table SRC_GENRES;
    COPY INTO SRC_GENRES
    FROM @word_counts_stage_external/genres.tsv.csv
        FORCE = TRUE;

    truncate table SRC_WORDS;
    COPY INTO SRC_WORDS
    FROM @word_counts_stage_external/unstemmed_mapping.txt.csv
    FORCE = TRUE;

    truncate table SRC_SONGS;
    COPY INTO SRC_SONGS
    FROM @word_counts_stage_external/id-track-artist.txt_v2.CSV
    ON_ERROR = CONTINUE -- that's ok to skip some records. don't have any processing of skipped, though good to have.
    FORCE = TRUE;

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

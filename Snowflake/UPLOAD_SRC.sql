
USE ROLE R_LYRICS_ANALYSIS;
USE WAREHOUSE WH_LYRICS_ANALYSIS;
USE DATABASE DB_LYRICS_ANALYSIS;
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
   
--select * from SRC_SONGS where MSD_TITLE like '%"%' limit 100;   
-- STEP 1: Create a Storage Integration to connect Snowflake with your S3 bucket
CREATE OR REPLACE STORAGE INTEGRATION s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'iam-role-snowflake-s3-connection-arn'  -- IAM role Snowflake will assume
  STORAGE_ALLOWED_LOCATIONS = ('s3-bucket-name')  -- Full S3 bucket path (e.g., 's3://my-bucket-name/')
  COMMENT = 'Creating Connection to S3';

-- Verify the integration and retrieve External ID (used in trust policy in AWS IAM)
DESC INTEGRATION s3_init;

-- STEP 2: Define the file format for the CSV files coming from Glue
CREATE OR REPLACE FILE FORMAT csv_fileformat
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

-- STEP 3: Create an external stage to point to the transformed data in S3
CREATE OR REPLACE STAGE spotify_stage
  URL = '<s3-bucket-name>/transformed-data/'  -- Example: 's3://my-bucket-name/transformed-data/'
  STORAGE_INTEGRATION = s3_init
  FILE_FORMAT = csv_fileformat;

-- List files in the album-data subfolder to verify stage visibility
LIST @spotify_stage/album-data;


-- STEP 4: Create target tables in Snowflake to store album and track data
CREATE OR REPLACE TABLE tbl_album (
  album_id STRING,
  album_name STRING,
  release_date DATE,
  num_of_tracks INTEGER,
  album_url STRING
);

CREATE OR REPLACE TABLE tbl_tracks (
  track_id STRING,
  track_name STRING,
  album_name STRING,
  album_id STRING,
  track_duration_ms INTEGER,
  is_explicit BOOLEAN,
  popularity INTEGER,
  track_url STRING
);


-- STEP 5: Create a separate schema to organize your Snowpipes
CREATE OR REPLACE SCHEMA data_pipes;

-- STEP 6: Create Snowpipe to automatically ingest album data from S3
CREATE OR REPLACE PIPE spotify_db.data_pipes.tbl_album_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO spotify_db.public.tbl_album
  FROM @spotify_db.public.spotify_stage/album-data;

-- Create Snowpipe to automatically ingest track data from S3
CREATE OR REPLACE PIPE spotify_db.data_pipes.tbl_tracks_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO spotify_db.public.tbl_tracks
  FROM @spotify_db.public.spotify_stage/tracks-data;

-- Inspect metadata for the album pipe (e.g., notification channel ARN)
DESC PIPE data_pipes.tbl_album_pipe;

-- STEP 7: Run test queries to validate ingestion

-- Count records in tracks table
SELECT COUNT(*) FROM tbl_tracks;

-- Check Snowpipe ingestion status
SELECT SYSTEM$PIPE_STATUS('data_pipes.tbl_album_pipe');

-- List files under the album-data folder in S3 via the stage
LIST @spotify_db.public.spotify_stage/album-data;

-- STEP 8: Sample query - count of albums by release date
SELECT release_date, COUNT(*) AS total_albums
FROM tbl_album
GROUP BY release_date
ORDER BY release_date DESC;

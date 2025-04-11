# Spotify ETL Pipeline with AWS and Snowflake

## 📌 Project Overview
This project is designed to automate the extraction, transformation, and loading (ETL) of music data from the Spotify API into a Snowflake data warehouse using AWS services. The goal is to enable scalable, automated analytics on Spotify track and album data. For a detailed walkthrough of the project, check out my [Medium article](https://medium.com/@KuldeepsinhVaghela/building-a-real-time-spotify-etl-pipeline-with-aws-and-snowflake-d7b360157097).

## 🧱 Architecture Overview
The ETL pipeline is built using the following components:

![Architecture Diagram](./architecture_diagram.png)

## 🛰️ Workflow Breakdown

### 🔄 EXTRACT
- **Trigger:** Amazon CloudWatch Event triggers every 1 minute.
- **AWS Lambda (Extraction):**
  - Authenticates with Spotify using the Spotipy library and credentials stored in environment variables.
  - Extracts top tracks for a given artist (e.g., Ed Sheeran).
  - Writes raw JSON data to:  
    `s3://<raw bucket path>`
  - Starts the AWS Glue transformation job.

### 🔧 TRANSFORM
- **AWS Glue Job:**
  - Reads raw JSON data from the raw S3 bucket.
  - Parses and explodes nested track and album JSON structures.
  - Creates two datasets:
    - **Albums:** album_id, album_name, release_date, num_of_tracks, album_url
    - **Tracks:** track_id, track_name, album_name, album_id, track_duration_ms, is_explicit, popularity, track_url
  - Writes output as CSV files to:
    - `s3://<transformed bucket path>album_data_transformed_<date>/`
    - `s3://<transformed bucket path>tracks_data_transformed_<date>/`

### 📥 LOAD (into Snowflake)
- **Storage Integration:**
  - Snowflake uses an external stage with access to the S3 bucket via a configured IAM role.

- **File Format:**
  - Defined CSV file format with `SKIP_HEADER=1`, and `empty_field_as_null=true`.

- **External Stage:**
  - Points to: `s3://<transformed bucket path>`

- **Tables:**
  - `tbl_album` and `tbl_tracks` created in Snowflake to store structured data.

- **Snowpipes:**
  - `tbl_album_pipe` watches `album-data/`
  - `tbl_tracks_pipe` watches `tracks-data/`
  - Auto-ingests new files when S3 event notifications are received.


## ✅ Monitoring & Validation
- View latest pipe status:
  ```sql
  SELECT SYSTEM$PIPE_STATUS('data_pipes.tbl_album_pipe');
  ```

- View loaded file history:
  ```sql
  SELECT *
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'SPOTIFY_DB.PUBLIC.TBL_ALBUM'))
  ORDER BY START_TIME DESC;
  ```

## 🧰 Tech Stack
- **Data Source:** Spotify Web API
- **Orchestration & Compute:** AWS Lambda, AWS Glue, Amazon CloudWatch
- **Storage:** Amazon S3
- **Data Warehouse:** Snowflake
- **Monitoring:** Snowflake COPY_HISTORY, SYSTEM$PIPE_STATUS

## 📂 S3 Folder Structure
```
s3://spotify-etl-project-kuldeep/
|
|-- raw-data/
|    |-- to-processed/
|
|-- transformed-data/
     |-- album-data/
     |    |-- album_data_transformed_<date>/
     |
     |-- tracks-data/
          |-- tracks_data_transformed_<date>/
```



> **Note:** All AWS resource names and paths have been generalized to avoid exposing sensitive or private identifiers.


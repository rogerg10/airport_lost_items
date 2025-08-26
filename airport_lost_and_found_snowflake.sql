-- ========================================================================
-- 1. Database, Schema, and Table Creation
-- ========================================================================
-- Establishes the foundational data structures and storage for the lost and found solution.
-- Creates the core tables and file format for storing found items, raw AI-enriched data, and user claims.

-- Create the main database and schema for lost and found items
CREATE DATABASE IF NOT EXISTS LOST_AND_FOUND;
CREATE SCHEMA IF NOT EXISTS LOST_AND_FOUND.LOST_ITEMS_SCHEMA;

-- Create a file format for ingesting JSON data
CREATE OR REPLACE FILE FORMAT lost_items_json_ff;

-- Table for basic info about found items
CREATE OR REPLACE TABLE LOST_AND_FOUND_ITEMS (
  filename STRING PRIMARY KEY,
  location STRING,
  found_time TIMESTAMP,
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table for storing raw item data, including AI-generated details
CREATE OR REPLACE TABLE LOST_AND_FOUND_ITEMS_RAW (
  id INTEGER AUTOINCREMENT PRIMARY KEY,
  filename STRING,
  classification STRING,
  location STRING,
  found_time TIMESTAMP,
  item_details VARIANT,
  etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table for user claims on lost items
CREATE OR REPLACE TABLE LOST_ITEM_CLAIMS (
  claim_id           STRING PRIMARY KEY DEFAULT UUID_STRING(),
  commentary         STRING,
  category           STRING,
  brand              STRING,
  terminal           STRING,
  gate               STRING,
  name               STRING,
  email              STRING,
  phone_number       STRING,
  helpdesk_location  STRING,
  status             STRING DEFAULT 'Outstanding',
  claim_lodged_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ========================================================================
-- 1a. Snowflake Warehouse Creation
-- ========================================================================
-- Create a dedicated XS warehouse (named LOST_AND_FOUND_WH) for automation and processing.
-- Auto-suspend enabled to minimize usage costs in public examples.
CREATE OR REPLACE WAREHOUSE LOST_AND_FOUND_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- ========================================================================
-- 2. Storage, Staging, and Data Ingestion
-- ========================================================================
-- Configures secure access to S3 storage and sets up automated data ingestion.
-- Enables automated ingestion of lost item data from S3 into Snowflake tables.

-- Create a storage integration for secure S3 access
CREATE OR REPLACE STORAGE INTEGRATION LOST_ITEMS_BLOB
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_ROLE_ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('<YOUR_S3_BUCKET_URL>');

-- Create an external stage pointing to the S3 bucket
CREATE OR REPLACE STAGE LOST_ITEMS_STAGE
  URL = '<YOUR_S3_BUCKET_URL>'
  STORAGE_INTEGRATION = LOST_ITEMS_BLOB
  DIRECTORY = (ENABLE = true);

-- List files in the stage to confirm connectivity
LIST @LOST_ITEMS_STAGE/lost_items/;

-- Create a Snowpipe for auto-ingesting new JSON files into the found items table
CREATE OR REPLACE PIPE LOST_AND_FOUND_ITEMS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO LOST_AND_FOUND_ITEMS
FROM @LOST_ITEMS_STAGE/lost_items/json/
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ========================================================================
-- 3. Data Processing and Automation
-- ========================================================================
-- Implements automation for processing, classifying, and enriching found item data using AI.
-- Automates the enrichment of found item data with AI-powered classification and description.

-- Create a stream to track new rows in the found items table
CREATE OR REPLACE STREAM LOST_AND_FOUND_ITEMS_STREAM
  ON TABLE LOST_AND_FOUND_ITEMS
  APPEND_ONLY = TRUE;

-- Create a task to process new found items, classify images, and generate AI descriptions
CREATE OR REPLACE TASK PROCESS_LOST_AND_FOUND_ITEMS_TASK
  WAREHOUSE = LOST_AND_FOUND_WH
  WHEN SYSTEM$STREAM_HAS_DATA('LOST_AND_FOUND_ITEMS_STREAM')
AS
INSERT INTO LOST_AND_FOUND_ITEMS_RAW (
  filename, classification, location, found_time, item_details
)
WITH tracked_items AS (
    SELECT filename, location, found_time
    FROM LOST_AND_FOUND_ITEMS_STREAM
),
lost_item_image AS (
    SELECT
        t.filename,
        t.location,
        t.found_time,
        TO_FILE('@LOST_AND_FOUND.LOST_ITEMS_SCHEMA.LOST_ITEMS_STAGE', 'lost_items/' || t.filename) AS img
    FROM tracked_items t
),
classified_image AS (
    SELECT
        filename,
        AI_CLASSIFY(
            img,
            ['bracelet', 'handbag', 'phone case', 'shoes', 'sunglasses', 'wallet', 'watch', 'keys', 'backpack', 'laptop', 'tablet', 'umbrella', 'hat', 'scarf', 'jacket', 'earphones', 'camera', 'book', 'water bottle', 'charger', 'passport', 'ID card', 'notebook', 'pen', 'gloves', 'ring', 'necklace', 't-shirt', 'shirt', 'badge', 'credit card', 'cosmetics', 'sunglasses case', 'batteries']
        ):labels[0] AS classification,
        location,
        found_time,
        img
    FROM lost_item_image
),
item_description AS (
    SELECT
        filename,
        classification,
        location,
        found_time,
        PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE(
            'claude-3-5-sonnet',
            'Describe the key characteristics of a lost ' || classification || ' as seen in this image. Respond in JSON with fields: item_type, color, brand (if visible), distinguishing_features, condition.',
            img
        )) AS item_details
    FROM classified_image
)
SELECT
    filename,
    classification,
    location,
    found_time,
    item_details
FROM item_description;

-- Resume the processing task
ALTER TASK PROCESS_LOST_AND_FOUND_ITEMS_TASK RESUME;

-- ========================================================================
-- 4. Matching Claims to Found Items
-- ========================================================================
-- Performs AI-driven similarity matching between found items and user claims.
-- Uses AI similarity to match found items with outstanding user claims based on category, location, and descriptive details.

-- Ad-hoc query for matching claims to found items

WITH found_item_details AS (
  SELECT
    filename,
    ITEM_DETAILS::STRING AS item_details_string,
    LOWER(classification) AS category_lower,
    LOWER(ITEM_DETAILS:brand::STRING) AS brand_lower,
    LOWER(TRIM(SPLIT_PART(location, ',', 1))) AS terminal_lower,
    LOWER(TRIM(SPLIT_PART(location, ',', 2))) AS gate_lower,
    found_time
  FROM LOST_AND_FOUND_ITEMS_RAW
  WHERE ITEM_DETAILS IS NOT NULL
),
outstanding_claims AS (
  SELECT
    claim_id,
    commentary,
    LOWER(category) AS category_lower,
    LOWER(brand) AS brand_lower,
    LOWER(terminal) AS terminal_lower,
    LOWER(gate) AS gate_lower,
    claim_lodged_at
  FROM LOST_ITEM_CLAIMS
  WHERE status = 'Outstanding'
)
SELECT
  i.filename,
  c.claim_id,
  c.commentary,
  ROUND(
    AI_SIMILARITY(
      i.brand_lower || ' ' || i.item_details_string,
      c.brand_lower || ' ' || c.commentary
    ) * 100, 2
  ) AS similarity_score_percent
FROM found_item_details i
JOIN outstanding_claims c
  ON i.category_lower = c.category_lower
  AND i.terminal_lower = c.terminal_lower
  AND i.gate_lower = c.gate_lower
-- WHERE
--   i.found_time >= DATEADD(day, -1, CAST(c.claim_lodged_at AS DATE))
--   AND i.found_time < DATEADD(day, 1, CAST(c.claim_lodged_at AS DATE))
ORDER BY similarity_score_percent DESC;

-- ------------------------------------------------------------------------
-- User-Defined Functions for Matching and Presigned URLs
-- ------------------------------------------------------------------------

-- Function: get_lost_item_matches
-- Returns the top 3 most similar found items for a given claim_id, using AI similarity.
CREATE OR REPLACE FUNCTION get_lost_item_matches(claim_id_param STRING)
RETURNS TABLE (
    filename STRING,
    found_time TIMESTAMP,
    item_details_string STRING,
    similarity_score_percent FLOAT
)
AS
$$
    WITH found_item_details AS (
      SELECT
        filename,
        ITEM_DETAILS::STRING AS item_details_string,
        LOWER(classification) AS category_lower,
        LOWER(ITEM_DETAILS:brand::STRING) AS brand_lower,
        LOWER(TRIM(SPLIT_PART(location, ',', 1))) AS terminal_lower,
        LOWER(TRIM(SPLIT_PART(location, ',', 2))) AS gate_lower,
        found_time
      FROM LOST_AND_FOUND_ITEMS_RAW
      WHERE ITEM_DETAILS IS NOT NULL
    ),
    outstanding_claims AS (
      SELECT
        claim_id,
        commentary,
        LOWER(category) AS category_lower,
        LOWER(brand) AS brand_lower,
        LOWER(terminal) AS terminal_lower,
        LOWER(gate) AS gate_lower,
        claim_lodged_at
      FROM LOST_ITEM_CLAIMS
      WHERE status = 'Outstanding'
    )
    SELECT
      i.filename,
      i.found_time,
      i.item_details_string,
      ROUND(
        AI_SIMILARITY(
          i.brand_lower || ' ' || i.item_details_string,
          c.brand_lower || ' ' || c.commentary
        ) * 100, 2
      ) AS similarity_score_percent
    FROM found_item_details i
    JOIN outstanding_claims c
      ON i.category_lower = c.category_lower
      AND i.terminal_lower = c.terminal_lower
      AND i.gate_lower = c.gate_lower
    WHERE c.claim_id = claim_id_param
      -- AND i.found_time >= DATEADD(day, -1, CAST(c.claim_lodged_at AS DATE))
      -- AND i.found_time < DATEADD(day, 1, CAST(c.claim_lodged_at AS DATE))
    ORDER BY similarity_score_percent DESC
    LIMIT 3
$$;

-- Function: get_lost_item_presigned_url
-- Returns a presigned URL for a given filename in the S3 stage, valid for 1 hour.
CREATE OR REPLACE FUNCTION get_lost_item_presigned_url(filename STRING)
RETURNS STRING
AS
$$
  SELECT GET_PRESIGNED_URL('@LOST_ITEMS_STAGE/lost_items/', filename, 3600)
$$;

-- ========================================================================
-- 5. Testing, Monitoring, and Cost Analysis
-- ========================================================================
-- Validates data flow, monitors automation, and provides cost transparency for AI operations.
-- List of queries to verify ingestion, processing, and matching, as well as to monitor automation and analyze AI function costs.

-- Check loaded found items
SELECT * FROM LOST_AND_FOUND_ITEMS;

-- Check processed raw items
SELECT * FROM LOST_AND_FOUND_ITEMS_RAW;

-- Check claims table
SELECT * FROM LOST_ITEM_CLAIMS;

-- Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('LOST_AND_FOUND_ITEMS_STREAM') AS stream_has_data;

-- Check Snowpipe status
SELECT SYSTEM$PIPE_STATUS('LOST_AND_FOUND.LOST_ITEMS_SCHEMA.LOST_AND_FOUND_ITEMS_PIPE');

-- Check task status
SHOW TASKS LIKE 'PROCESS_LOST_AND_FOUND_ITEMS_TASK';

-- Confirm row counts in main and raw tables
SELECT COUNT(*) AS row_count FROM LOST_AND_FOUND_ITEMS;
SELECT COUNT(*) AS row_count FROM LOST_AND_FOUND_ITEMS_RAW;

-- Confirm S3 stage contents
LIST @LOST_ITEMS_STAGE/lost_items/json/;

-- Per-query usage and cost for AI functions
  SELECT
    c.QUERY_ID,
    c.FUNCTION_NAME,
    c.MODEL_NAME,
    c.TOKENS,
    c.TOKEN_CREDITS,
    c.TOKEN_CREDITS * 3 AS DOLLAR_COST, -- Calculates dollar cost per query at $3 per credit (Snowflake Enterprise Edition, AWS Oregon)
    DATEDIFF('second', q.START_TIME, q.END_TIME) AS DURATION_SECONDS,
    CONVERT_TIMEZONE('UTC', 'Australia/Sydney', q.START_TIME) AS START_TIME_SYDNEY,
    CONVERT_TIMEZONE('UTC', 'Australia/Sydney', q.END_TIME) AS END_TIME_SYDNEY
  FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY c
  JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
    ON c.QUERY_ID = q.QUERY_ID
  WHERE c.FUNCTION_NAME IN ('AI_CLASSIFY', 'AI_SIMILARITY', 'COMPLETE')
    AND q.START_TIME >= DATEADD('day', -10, CURRENT_TIMESTAMP());


    

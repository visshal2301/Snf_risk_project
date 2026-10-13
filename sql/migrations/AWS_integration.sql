use RISK_DEV_DB.RAW;
-- 1. Create a reusable GZIP CSV file format
CREATE OR REPLACE FILE FORMAT ff_csv_gzip
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- 2. Create file-specific stages based on our inventory structure
CREATE OR REPLACE STAGE stg_country_files 
  URL = 's3://sfquickstarts/tastybytes/raw_pos/country/';

  -- 2. Automatically infer the schema of the file
SELECT * 
FROM TABLE(
  INFER_SCHEMA(
    LOCATION => '@stg_country_files/country.csv.gz',
    FILE_FORMAT => 'ff_csv_gzip'
  )
);

SELECT 
        $1, 
        $2,
        $3,
        $4,
        $5,
        $6,
        $7
    FROM @stg_country_files/country.csv.gz;

CREATE OR REPLACE STAGE stg_location_files 
  URL = 's3://sfquickstarts/tastybytes/raw_pos/location/';

CREATE OR REPLACE STAGE STG_ORDER_HEADER_FILES 
  URL = 's3://sfquickstarts/tastybytes/raw_pos/order_detail/';

CREATE OR REPLACE STAGE stg_order_detail_files 
  URL = 's3://sfquickstarts/tastybytes/raw_pos/order_header/';

  
  -- Create dim_location Dimension Table
CREATE TABLE IF NOT EXISTS RISK_DEV_DB.ANALYTICS.dim_location (
    location_key NUMBER AUTOINCREMENT,
    location_id VARCHAR,
    city VARCHAR,
    country VARCHAR,
    effective_start_date DATE,
    effective_end_date DATE,
    is_active BOOLEAN,
    updated_at TIMESTAMP_NTZ
);

-- Daily Incremental Routine for Dimension (File-specific & Idempotent via Staging Truncate)
CREATE OR REPLACE TRANSIENT TABLE RISK_DEV_DB.RAW.stg_daily_location_delta (
    location_id VARCHAR,
    city VARCHAR,
    country VARCHAR
);

-- Clear staging and load the specific daily file
TRUNCATE TABLE  RISK_DEV_DB.RAW.stg_daily_location_delta;
COPY INTO  RISK_DEV_DB.RAW.stg_daily_location_delta (location_id, city, country)
FROM @stg_location_files/location.csv.gz
FILE_FORMAT = ff_csv_gzip;

-- 1. Close out (expire) records that have changed (SCD Type 2)
UPDATE RISK_DEV_DB.ANALYTICS.dim_location target
SET target.effective_end_date = CURRENT_DATE() - INTERVAL '1 DAY',
    target.is_active = FALSE,
    target.updated_at = CURRENT_TIMESTAMP()
FROM RISK_DEV_DB.RAW.stg_daily_location_delta source
WHERE target.location_id = source.location_id
  AND target.is_active = TRUE
  AND (target.city <> source.city OR target.country<> source.country) ;

-- 2. Insert new versions or brand-new records
INSERT INTO RISK_DEV_DB.ANALYTICS.dim_location (location_id, city, country, effective_start_date, effective_end_date, is_active, updated_at)
SELECT 
    s.location_id,
    s.city,
    s.country,
    CURRENT_DATE() AS effective_start_date,
    '9999-12-31'::DATE AS effective_end_date,
    TRUE AS is_active,
    CURRENT_TIMESTAMP()
FROM RISK_DEV_DB.RAW.stg_daily_location_delta s
LEFT JOIN RISK_DEV_DB.ANALYTICS.dim_location t 
  ON s.location_id = t.location_id AND t.is_active = TRUE
WHERE t.location_id IS NULL 
   OR t.city <> s.city;


select * from RISK_DEV_DB.RAW.stg_daily_location_delta s;

select * from RISK_DEV_DB.ANALYTICS.dim_location s where location_id=1002;


update RISK_DEV_DB.RAW.stg_daily_location_delta set city='Pune'
where location_id=1002;


--create fact table

-- 1. Ensure your target table has the correct matching columns
-- (Run this once if your table columns don't match the 11 columns below)
CREATE OR REPLACE TABLE RISK_DEV_DB.ANALYTICS.fact_order_detail (
    order_detail_id VARCHAR,
    order_id VARCHAR,
    order_line_id VARCHAR,
    menu_item_id VARCHAR,
    quantity VARCHAR,
    unit_price VARCHAR,
    price VARCHAR,
    discount_amount VARCHAR,
    tax_amount VARCHAR,
    source_file_name VARCHAR,
    order_date TIMESTAMP_NTZ
);

-- 2. Execution Run Parameters
SET target_batch_date = '2026-10-04';

-- 3. IDEMPOTENCY STEP: Delete existing rows for this date
DELETE FROM RISK_DEV_DB.ANALYTICS.fact_order_detail 
WHERE CAST(order_date AS DATE) = $target_batch_date;

-- 4. FINAL COPY INTO INSERT QUERY
COPY INTO RISK_DEV_DB.ANALYTICS.fact_order_detail (
    order_detail_id, 
    order_id, 
    order_line_id, 
    menu_item_id, 
    quantity, 
    unit_price, 
    price, 
    discount_amount, 
    tax_amount, 
    source_file_name,
    order_date
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        $4, 
        $5, 
        $6, 
        $7, 
        $8, 
        $9, 
        METADATA$FILENAME,
        CAST($target_batch_date AS TIMESTAMP_NTZ)
    FROM @stg_order_detail_files/order_detail_0_1_0.csv.gz
)
FILE_FORMAT = ff_csv_gzip 
ON_ERROR = 'ABORT_STATEMENT';

select count(*),order_date from RISK_DEV_DB.ANALYTICS.fact_order_detail group by all;

select * from RISK_DEV_DB.ANALYTICS.fact_order_detail;

SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));



   
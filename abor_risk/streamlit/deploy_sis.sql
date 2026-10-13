-- ==============================================================================
-- 🏛️ Institutional ABOR & Risk Platform: Streamlit in Snowflake Deployment
-- ==============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE RISK_DEV_DB;

-- 1. Create Internal Stage for Streamlit code artifacts
CREATE STAGE IF NOT EXISTS ENG.STREAMLIT_STAGE
DIRECTORY = (ENABLE = TRUE);

-- 2. Stage Upload Commands (Run via SnowSQL CLI or Web UI Stage upload)
-- PUT file://abor_risk/streamlit/risk_dashboard.py @ENG.STREAMLIT_STAGE OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://abor_risk/streamlit/styles.py @ENG.STREAMLIT_STAGE OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://abor_risk/streamlit/data_service.py @ENG.STREAMLIT_STAGE OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://abor_risk/streamlit/contagion_engine.py @ENG.STREAMLIT_STAGE OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- 3. Create or Replace Streamlit in Snowflake App
CREATE OR REPLACE STREAMLIT ANALYTICS.RISK_COMMAND_CENTER
ROOT_LOCATION = '@ENG.STREAMLIT_STAGE'
MAIN_FILE = 'risk_dashboard.py'
QUERY_WAREHOUSE = 'COMPUTE_WH'
COMMENT = 'Institutional ABOR & Quantitative Risk Command Center Dashboard';

-- 4. Grant Usage to relevant roles
GRANT USAGE ON STREAMLIT ANALYTICS.RISK_COMMAND_CENTER TO ROLE ACCOUNTADMIN;

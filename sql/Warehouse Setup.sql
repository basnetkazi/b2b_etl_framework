USE Role sysadmin;


CREATE WAREHOUSE DWH_BATCH WITH WAREHOUSE_SIZE = 'SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 10 AUTO_RESUME = TRUE COMMENT = 'BATCH WAREHOUSE';
GRANT MONITOR, OPERATE, USAGE ON WAREHOUSE DWH_BATCH TO ROLE DWH_BATCH;
CREATE WAREHOUSE DWH_USER WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 10 AUTO_RESUME = TRUE COMMENT = 'B2B Database Data Access individual user';
GRANT MONITOR, OPERATE, USAGE ON WAREHOUSE DWH_USER TO ROLE DWH_USER;
CREATE WAREHOUSE DWH_REPORT WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 10 AUTO_RESUME = TRUE COMMENT = 'B2B Reporting User';
GRANT MONITOR, OPERATE, USAGE ON WAREHOUSE DWH_REPORT TO ROLE DWH_REPORT;
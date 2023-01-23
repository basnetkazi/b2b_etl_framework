
CREATE OR REPLACE ROLE "DWH_BATCH" COMMENT = 'Role for Batch Execution DWH_BATCH';
GRANT ROLE "DWH_BATCH" TO ROLE "SYSADMIN";

CREATE ROLE "DWH_USER" COMMENT = 'Role for Prod Data Access';
GRANT ROLE "DWH_USER" TO ROLE "SYSADMIN";

CREATE ROLE "DWH_REPORT" COMMENT = 'Role to Read Access to Reporting Views';
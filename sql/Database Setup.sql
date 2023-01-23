Create Database B2B_PLATFORM;

grant usage on database B2B_PLATFORM to role DWH_BATCH;
grant all on all schemas in database B2B_PLATFORM to role DWH_BATCH;
grant all on all tables in database B2B_PLATFORM to role DWH_BATCH;
grant all on all views in database B2B_PLATFORM to role DWH_BATCH;
grant all on future views in database B2B_PLATFORM to role DWH_BATCH;
grant all on future tables in database B2B_PLATFORM to role DWH_BATCH;
grant all on all tasks in database B2B_PLATFORM to role DWH_BATCH;
grant usage on all procedures in database B2B_PLATFORM to role DWH_BATCH;
grant all on all File FORMATS in DATABASE B2B_PLATFORM to role DWH_BATCH;
grant all on future File FORMATS in DATABASE B2B_PLATFORM to role DWH_BATCH;






grant usage on database B2B_PLATFORM to role DWH_USER;
grant usage on all schemas in database B2B_PLATFORM to role DWH_USER;
grant select,update,insert,delete on all tables in database B2B_PLATFORM to role DWH_USER;
grant select,update,insert,delete on all views in database B2B_PLATFORM to role DWH_USER;
grant select on future views in database B2B_PLATFORM to role DWH_USER;
grant select,update,insert,delete on future tables in database B2B_PLATFORM to role DWH_USER;
grant all on all tasks in database B2B_PLATFORM to role DWH_USER;
grant usage on all procedures in database B2B_PLATFORM to role DWH_USER;


grant usage on database B2B_PLATFORM to role DWH_REPORT;
grant usage on  schema B2B_PLATFORM.DWH_REPORT   to role DWH_REPORT;
grant select on all views in schema B2B_PLATFORM.DWH_REPORT to role DWH_REPORT;
grant select on future views in schema B2B_PLATFORM.DWH_REPORT to role DWH_REPORT;




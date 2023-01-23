from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def company_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'Company_ex')
    v.set("TABLE",'COMPANY')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor = BatchMonitor(sqls, v, log, 'EXTRACTION')

    try:
        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            print("Company Extraction Started " + str(datetime.datetime.now()))
            log.log_message("Company Extraction Started " + str(datetime.datetime.now()))
            #create stage file for company table
            sqls.create_data_file(v.get("TABLE"))
            print("Company Extraction Finished " + str(datetime.datetime.now()))
            log.log_message("Company Extraction Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Company Extraction Failed",e)
        log.log_message("Company Extraction Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

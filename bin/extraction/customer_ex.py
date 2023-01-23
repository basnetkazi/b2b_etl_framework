from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor

def customer_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'Customer_ex')
    v.set("TABLE",'CUSTOMER')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'EXTRACTION')
    try:
        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            print("Customer Extraction Started " + str(datetime.datetime.now()))
            log.log_message("Customer Extraction Started " + str(datetime.datetime.now()))
            #create stage file for customer table
            sqls.create_data_file(v.get("TABLE"))
            print("Customer Extraction Finished " + str(datetime.datetime.now()))
            log.log_message("Customer Extraction Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Customer Extraction Failed",e)
        log.log_message("Customer Extraction Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e

    finally:
        sqls.end_connection()
        log.close()

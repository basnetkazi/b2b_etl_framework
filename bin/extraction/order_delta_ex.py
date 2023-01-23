from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor

def order_delta_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'Order_Delta_Ex')
    v.set("TABLE",'ORDERS')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'EXTRACTION')
    try:
        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            print("Order Extraction Started " + str(datetime.datetime.now()))
            log.log_message("Order Extraction Started " + str(datetime.datetime.now()))
            #create stage file for Order table
            sqls.create_data_file_delta(v.get("TABLE"))
            print("Order Extraction Finished " + str(datetime.datetime.now()))
            log.log_message("Order Extraction Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Order Extraction Failed",e)
        log.log_message("Order Extraction Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

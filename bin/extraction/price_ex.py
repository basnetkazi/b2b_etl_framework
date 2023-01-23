from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def price_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'Price_ex')
    v.set("TABLE",'PRICE')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'EXTRACTION')

    try:
        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            print("Price Extraction Started " + str(datetime.datetime.now()))
            log.log_message("Price Extraction Started " + str(datetime.datetime.now()))
            #create stage file for price table
            sqls.create_data_file(v.get("TABLE"))
            print("Price Extraction Finished " + str(datetime.datetime.now()))
            log.log_message("Price Extraction Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Price Extraction Failed",e)
        log.log_message("Price Extraction Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

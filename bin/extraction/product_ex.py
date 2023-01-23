from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def product_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'Product_ex')
    v.set("TABLE",'PRODUCT')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'EXTRACTION')

    try:

        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            #create stage file for product table
            print("Product Extraction Started " + str(datetime.datetime.now()))
            log.log_message("Product Extraction Started " + str(datetime.datetime.now()))
            sqls.create_data_file(v.get("TABLE"))
            print("Product Extraction Finished " + str(datetime.datetime.now()))
            log.log_message("Product Extraction Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Product Extraction Failed",e)
        log.log_message("Product Extraction Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

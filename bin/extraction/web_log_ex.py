from lib.Variables import Variables
import datetime
from os import  path
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def web_log_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'web_log_ex')
    v.set("TABLE",'LOG_LOAD')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'EXTRACTION')

    try:

        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            #create stage file for product table
            print("Web Log Extraction Started " + str(datetime.datetime.now()))
            log.log_message("Web Log Extraction Started " + str(datetime.datetime.now()))
            weblog_file_path=path.join(v.get("WEBLOG"),'weblog_*.log')
            # weblog_file_path= v.get("WEBLOG")+'\\weblog_*.log'
            query = "REMOVE @" + v.get("ISTAGE") + " pattern='.*.log';"
            log.log_message("Previous Log files cleare from stage sucessfully " + str(datetime.datetime.now()))
            sqls.execute_query(query)
            query="PUT 'file://"+weblog_file_path+"' @"+ v.get("ISTAGE")+"/"+" AUTO_COMPRESS = FALSE OVERWRITE = "+v.get("OVERWRITE_FLAG")+" ;"
            sqls.execute_query(query)
            print("Web Log Loaded into Stage File Sucessfully " + str(datetime.datetime.now()))
            log.log_message("Web Log Loaded into Stage File Sucessfully " + str(datetime.datetime.now()))
            #sqls.create_data_file(v.get("TABLE"))
            print("Web Log Extraction Finished " + str(datetime.datetime.now()))
            log.log_message("Web Log Extraction Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Web Log  Extraction Failed",e)
        log.log_message("Web Log  Extraction Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor

def start_load():
    v = Variables()
    v.set("SCRIPT_NAME", 'load_batch')
    v.set("TABLE",'DWH_BATCH_LOG')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'LOAD')
    try:
        log.log_message("Load Batch Started")
        monitor.get_job_id();
        batch_id, last_run_status, last_run_bookmark = monitor.get_last_status()
        if last_run_status <= 0:
            #sqls.truncate_table(v.get("DATABASE"), v.get("TARGET_SCHEMA"), v.get("TABLE"))
            query = """
                                       INSERT INTO """ + v.get("TARGET_SCHEMA") + """.""" + v.get("TABLE") + """  (
                                                           BATCH_ID
                                                          ,JOB_ID
                                                          ,JOB_NAME
                                                          ,RUN_DATE
                                                          ,STRT_TMS
                                                          ,END_TMS
                                                          ,STATUS
                                                          ,ERROR_MESSAGE
                                                          ,BOOKMARK
                                                          ,LOGFILE)
                                                      SELECT """ + str(batch_id + 1) + """
                                                           ,""" + str(monitor.get_job_id()) + """
                                                           ,'""" + v.get("SCRIPT_NAME") + """'
                                                            ,CURRENT_DATE
                                                            ,CURRENT_TIMESTAMP
                                                            ,NULL
                                                            ,'STARTED'
                                                            ,''
                                                            ,'' 
                                                            ,'""" + v.get("LOG_PATH") + """'
                                                       FROM DUAL"""
            sqls.execute_query(query)
        else:
            log.log_message("Please complete the previous batch or mark it as completed.")

        log.log_message("Script completed successfully.")

    except Exception as e:
        print("Load Batch Initiation Failed",e)
        log.log_message("Load Batch Initiation Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

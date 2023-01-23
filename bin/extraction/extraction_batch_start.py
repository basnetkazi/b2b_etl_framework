from lib.Variables import Variables
import datetime
import shutil, ntpath
import os
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor

def start_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'extraction_batch')
    v.set("TABLE",'SRC_BATCH_LOG')
    file_path=v.get("LOG_PATH")
    files = os.listdir(file_path)
    folder = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.mkdir(os.path.join(v.get("ARCHIVE_DIR"),folder))
    for f in files:
        f_arch = str(datetime.date.today()) + """_""" + ntpath.basename(f)
        shutil.move(file_path+'/'+f, v.get("ARCHIVE_DIR") + "/"+folder+"/" + f_arch)
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'EXTRACTION')
    try:
        log.log_message("Extraction Batch Started")
        monitor.get_job_id();
        batch_id, last_run_status, last_run_bookmark = monitor.get_last_status()
        if last_run_status<=0:
            #sqls.truncate_table(v.get("DATABASE"), v.get("SOURCE_SCHEMA"), v.get("TABLE"))
            query = """
                                INSERT INTO """ + v.get("SOURCE_SCHEMA") + """.""" + v.get("TABLE") + """  (
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
        print("Extraction Batch Initiation Failed",e)
        log.log_message("Extraction Batch Initiation Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

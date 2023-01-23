from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def end_extract():
    v = Variables()
    v.set("SCRIPT_NAME", 'extraction_batch')
    v.set("TABLE", 'SRC_BATCH_LOG')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor = BatchMonitor(sqls, v, log, 'EXTRACTION')
    try:
        v.set("JOB_ID",monitor.get_job_id())
        v.set("BATCH_ID", monitor.get_batch_id())
        monitor.audit_complete()
        log.log_message("Extraction Batch Completed")

    except Exception as e:
        print("Extraction Batch Initiation Failed", e)
        log.log_message("Extraction Batch Initiation Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e
    finally:
        sqls.end_connection()
        log.close()

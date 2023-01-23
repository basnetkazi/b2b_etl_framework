from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def web_log_load():
    v = Variables()
    v.set("SCRIPT_NAME", 'web_log_ld')
    v.set("TABLE","STG_WEB_LOG_DATA")
    v.set("TARGET_TABLE","DWH_WEB_LOG_DATA")
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'LOAD')

    try:
        monitor.start_audit()
        if v.get("BOOKMARK")=="NULL":
            print("Weblog Loading Started " + str(datetime.datetime.now()))
            log.log_message("Weblog Loading Started " + str(datetime.datetime.now()))

            log.log_message("LOADING THE Weblog TABLE FROM WEBLOG FILE")
            log.log_message("------------*****STARTED******-----------")



            sqls.load_stage_from_log(v.get("TABLE"))
            log.log_message(f"Stage table Load Completed Sucessfully ")

            log.log_message("LOADING THE STAGE TABLE FROM WEBLOG FILE")
            log.log_message("------------*****COMPLETED******-----------")

            monitor.set_bookmark("AFTER_STG_LOAD")

            if v.get("BOOKMARK") == "AFTER_STG_LOAD":
                log.log_message("INSERTING THE TARGET TABLE FROM STAGE TABLE")
                log.log_message("------------*****STARTED******-----------")
                load_tgt_table = f"""INSERT INTO {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")}(
                                IP
                                ,IDENTITY
                                ,USERNAME
                                ,TIME
                                ,REQUEST
                                ,STATUS
                                ,SIZE
                                ,REFERER
                                ,AGENT
                                ,RCD_INSERT_DT
                                ,RCD_UPDATE_DT
                            ) SELECT
                               IP
                            ,IDENTITY
                            ,USERNAME
                            ,to_timestamp_TZ(LTRIM(TIME, '[') || ' ' || RTRIM(TIME_ZONE, ']'), 'DD/MON/YYYY:HH24:MI:SS TZHTZM')
                            ,REQUEST
                            ,STATUS
                            ,SIZE
                            ,REFERER
                            ,AGENT
                            ,CURRENT_TIMESTAMP
                            ,CURRENT_TIMESTAMP
                            FROM {v.get("DATABASE")}.{v.get("STAGE_SCHEMA")}.{v.get("TABLE")}
               """

                sqls.load_table(load_tgt_table, v.get("TARGET_TABLE"), v.get("TARGET_SCHEMA"))

                log.log_message("INSERTING THE TARGET TABLE FROM TEMP TABLE")
                log.log_message("------------*****COMPLETED******-----------")

                print("Product Loading Finished " + str(datetime.datetime.now()))
                log.log_message("Product Loading Finished " + str(datetime.datetime.now()))
                monitor.audit_complete()

    except Exception as e:
        print("Product Loading Failed",e)
        log.log_message("Product Loading Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e

    finally:
        sqls.end_connection()
        log.close()

from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def company_load():
    v = Variables()
    v.set("SCRIPT_NAME", 'Company_ld')
    v.set("TABLE", "COMPANY")
    v.set("TEMP_TABLE", "TMP_COMPANY_D")
    v.set("REJECTION_TABLE", "REJ_COMPANY_D")
    v.set("TARGET_TABLE", "DWH_COMPANY_D")
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor = BatchMonitor(sqls, v, log, 'LOAD')
    try:
        monitor.start_audit()
        if v.get("BOOKMARK") == "NULL":
            print("Company Loading Started " + str(datetime.datetime.now()))
            log.log_message("Company Loading Started " + str(datetime.datetime.now()))

            log.log_message("LOADING THE STAGE TABLE FROM STAGE FILE")
            log.log_message("------------*****STARTED******-----------")

            sqls.load_stage_from_file(v.get("TABLE"))
            log.log_message(f"Stage table Load Completed Sucessfully ")

            log.log_message("LOADING THE STAGE TABLE FROM STAGE FILE")
            log.log_message("------------*****COMPLETED******-----------")

            monitor.set_bookmark("AFTER_STG_LOAD")
        if v.get("BOOKMARK") == "AFTER_STG_LOAD":
            log.log_message("LOADING THE TEMP TABLE FROM STAGE TABLE")
            log.log_message("------------*****STARTED******-----------")

            log.log_message("Temp table load  Started " + str(datetime.datetime.now()))
            sqls.truncate_table(v.get("DATABASE"), v.get("TEMP_SCHEMA"), v.get("TEMP_TABLE"))

            temp_load = f"""INSERT INTO {v.get("DATABASE")}.{v.get("TEMP_SCHEMA")}.{v.get("TEMP_TABLE")}
                            SELECT distinct CU_ID
                                ,NAME
                                ,SUPPLIER_FLAG
                                ,LOGIN_NAME
                                ,COUNTRY
                                ,LOCATION
                            FROM {v.get("DATABASE")}.{v.get("SOURCE_SCHEMA")}.{v.get("TABLE")} """

            result = sqls.load_table(temp_load, v.get("TEMP_TABLE"), v.get("TEMP_SCHEMA"))
            log.log_message("Temp table load  Completed " + str(datetime.datetime.now()))

            log.log_message("LOADING THE TEMP TABLE FROM STAGE TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_TMP_LOAD")

        if v.get("BOOKMARK") == "AFTER_TMP_LOAD":
            log.log_message("LOADING THE REJECTION TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")

            log.log_message("Rejection table load  Started " + str(datetime.datetime.now()))
            sqls.truncate_table(v.get("DATABASE"), v.get("REJECTION_SCHEMA"), v.get("REJECTION_TABLE"))

            temp_load = f"""INSERT INTO {v.get("DATABASE")}.{v.get("REJECTION_SCHEMA")}.{v.get("REJECTION_TABLE")}
                                    SELECT *
                                    FROM {v.get("DATABASE")}.{v.get("TEMP_SCHEMA")}.{v.get("TEMP_TABLE")} where CU_ID is null"""

            sqls.load_table(temp_load, v.get("REJECTION_TABLE"), v.get("REJECTION_SCHEMA"))
            log.log_message("Rejection table load  Completed " + str(datetime.datetime.now()))

            log.log_message("LOADING THE REJECTION TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_REJ_LOAD")

        if v.get("BOOKMARK") == "AFTER_REJ_LOAD":
            log.log_message("UPDATING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")
            update_tgt_table = f""" UPDATE {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} AS T1
                                                       SET T1.NAME = T2.NAME ,
                                                       T1.SUPPLIER_FLAG = T2.SUPPLIER_FLAG,
                                                       T1.LOGIN_NAME = T2.LOGIN_NAME,
                                                       T1.COUNTRY = T2.COUNTRY ,
                                                       T1.LOCATION = T2.LOCATION ,
                                                       RCD_UPDATE_DT = LOCALTIMESTAMP
                                                       FROM {v.get("DATABASE")}.{v.get("TEMP_SCHEMA")}.{v.get("TEMP_TABLE")} AS T2
                                                       WHERE T1.CU_ID = T2.CU_ID
													   AND (NVL(T1.NAME, '?') <> NVL(T2.NAME, '?')
                                                            OR NVL(T1.SUPPLIER_FLAG, '?') <> NVL(T2.SUPPLIER_FLAG, '?')
                                                            OR NVL(T1.LOGIN_NAME, '?') <> NVL(T2.LOGIN_NAME, '?')
                                                            OR NVL(T1.COUNTRY, '?') <> NVL(T2.COUNTRY, '?')
                                                            OR NVL(T1.LOCATION, '?') <> NVL(T2.LOCATION, '?')
                                                            );
                               """
            sqls.load_table(update_tgt_table, v.get("TARGET_TABLE"), v.get("TARGET_SCHEMA"))

            log.log_message("Updating target table from Temp Completed " + str(datetime.datetime.now()))

            log.log_message("UPDATING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_TARGET_UPDATE")

        if v.get("BOOKMARK") == "AFTER_TARGET_UPDATE":
            log.log_message("INSERTING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")
            load_tgt_table = f"""INSERT INTO {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")}(
                            CU_ID,
                            NAME,
                            SUPPLIER_FLAG,
                            LOGIN_NAME,
                            COUNTRY,
                            LOCATION,                           
                            CURRENT_FLG,
                            RCD_INSERT_DT,
                            RCD_UPDATE_DT
                        ) SELECT
                            CU_ID,
                            NAME,
                            SUPPLIER_FLAG,
                            LOGIN_NAME,
                            COUNTRY,
                            LOCATION, 
                            'Y',
                            LOCALTIMESTAMP,
                            LOCALTIMESTAMP
                        FROM {v.get("DATABASE")}.{v.get("TEMP_SCHEMA")}.{v.get("TEMP_TABLE")}
            WHERE CU_ID IS NOT NULL AND
            CU_ID NOT IN (SELECT DISTINCT CU_ID from {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} )"""

            sqls.load_table(load_tgt_table, v.get("TARGET_TABLE"), v.get("TARGET_SCHEMA"))

            log.log_message("INSERTING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            print("Company Loading Finished " + str(datetime.datetime.now()))
            log.log_message("Company Loading Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Company Loading Failed", e)
        log.log_message("Company Loading Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e

    finally:
        sqls.end_connection()
        log.close()

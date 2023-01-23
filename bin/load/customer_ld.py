from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor

def customer_load():
    v = Variables()
    v.set("SCRIPT_NAME", 'Customer_ld')
    v.set("TABLE","CUSTOMER")
    v.set("TEMP_TABLE","TMP_CUSTOMER_D")
    v.set("REJECTION_TABLE","REJ_CUSTOMER_D")
    v.set("TARGET_TABLE","DWH_CUSTOMER_D")
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'LOAD')
    try:
        monitor.start_audit()
        if v.get("BOOKMARK")=="NULL":
            print("Customer Loading Started " + str(datetime.datetime.now()))
            log.log_message("Customer Loading Started " + str(datetime.datetime.now()))

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
            sqls.truncate_table(v.get("DATABASE"),v.get("TEMP_SCHEMA"), v.get("TEMP_TABLE"))

            temp_load=f"""INSERT INTO {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") }
                            SELECT distinct DO_ID
                                ,FIRST_NAME
                                ,FULL_NAME
                                ,LAST_NAME
                                ,DOB
                                ,LOGIN_NAME
                                ,COUNTRY
                                ,LOCATION
                            FROM {v.get("DATABASE") }.{v.get("SOURCE_SCHEMA") }.{v.get("TABLE") } """

            result =sqls.load_table(temp_load, v.get("TEMP_TABLE"), v.get("TEMP_SCHEMA"))
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
                                    FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } where DO_ID is null"""

            sqls.load_table(temp_load, v.get("REJECTION_TABLE"), v.get("REJECTION_SCHEMA"))
            log.log_message("Rejection table load  Completed " + str(datetime.datetime.now()))

            log.log_message("LOADING THE REJECTION TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_REJ_LOAD")

        if v.get("BOOKMARK") == "AFTER_REJ_LOAD":
            log.log_message("UPDATING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")
            update_tgt_table = f""" UPDATE {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} AS T1
                                                       SET T1.FIRST_NAME = T2.FIRST_NAME ,
                                                       T1.FULL_NAME = T2.FULL_NAME,
                                                       T1.LAST_NAME = T2.LAST_NAME,
                                                       T1.DOB = T2.DOB,
                                                       T1.LOGIN_NAME = T2.LOGIN_NAME,
                                                       T1.COUNTRY = T2.COUNTRY,
                                                       T1.LOCATION = T2.LOCATION,
                                                       RCD_UPDATE_DT = LOCALTIMESTAMP
                                                       FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } AS T2
                                                       WHERE T1.DO_ID = T2.DO_ID
                                                       AND
													   (NVL(T1.FIRST_NAME, '?') <> NVL(T2.FIRST_NAME, '?')
                                                        OR NVL(T1.FULL_NAME, '?') <> NVL(T2.FULL_NAME, '?')
                                                        OR NVL(T1.LAST_NAME, '?') <> NVL(T2.LAST_NAME, '?')
                                                        OR NVL(T1.DOB, '?') <> NVL(T2.DOB, '?')
                                                        OR NVL(T1.LOGIN_NAME, '?') <> NVL(T2.LOGIN_NAME, '?')
                                                        OR NVL(T1.COUNTRY, '?') <> NVL(T2.COUNTRY, '?')
                                                        OR NVL(T1.LOCATION, '?') <> NVL(T2.LOCATION, '?'));
                               """
            sqls.load_table(update_tgt_table, v.get("TARGET_TABLE"), v.get("TARGET_SCHEMA"))

            log.log_message("Updating target table from Temp Completed " + str(datetime.datetime.now()))

            log.log_message("UPDATING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_TARGET_UPDATE")


        if v.get("BOOKMARK")=="AFTER_TARGET_UPDATE":
            log.log_message("INSERTING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")
            load_tgt_table = f"""INSERT INTO {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")}(
                             DO_ID
                            ,FIRST_NAME
                            ,FULL_NAME
                            ,LAST_NAME
                            ,DOB
                            ,LOGIN_NAME
                            ,COUNTRY
                            ,LOCATION
                            ,CURRENT_FLG
                            ,RCD_INSERT_DT
                            ,RCD_UPDATE_DT
                        ) SELECT
                             DO_ID
                            ,FIRST_NAME
                            ,FULL_NAME
                            ,LAST_NAME
                            ,DOB
                            ,LOGIN_NAME
                            ,COUNTRY
                            ,LOCATION
                            ,'Y'
                            ,LOCALTIMESTAMP
                            ,LOCALTIMESTAMP
                        FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") }
            WHERE DO_ID IS NOT NULL AND
            DO_ID NOT IN (SELECT DISTINCT DO_ID from {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} )"""

            sqls.load_table(load_tgt_table, v.get("TARGET_TABLE"), v.get("TARGET_SCHEMA"))

            log.log_message("INSERTING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            print("Customer Loading Finished " + str(datetime.datetime.now()))
            log.log_message("Customer Loading Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Customer Loading Failed",e)
        log.log_message("Customer Loading Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e

    finally:
        sqls.end_connection()
        log.close()

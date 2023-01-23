from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor


def product_load():
    v = Variables()
    v.set("SCRIPT_NAME", 'Product_ld')
    v.set("TABLE","PRODUCT")
    v.set("TEMP_TABLE","TMP_PRODUCT_D")
    v.set("REJECTION_TABLE","REJ_PRODUCT_D")
    v.set("TARGET_TABLE","DWH_PRODUCT_D")
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'LOAD')

    try:
        monitor.start_audit()
        if v.get("BOOKMARK")=="NULL":
            print("Product Loading Started " + str(datetime.datetime.now()))
            log.log_message("Product Loading Started " + str(datetime.datetime.now()))

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
                            SELECT distinct PROD_ID
                                ,DESCRIPTION
                                ,DEPARTMENT
                                ,CLASS
                            FROM {v.get("DATABASE") }.{v.get("SOURCE_SCHEMA") }.{v.get("TABLE") }"""

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
                                    FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } where PROD_ID is null"""

            sqls.load_table(temp_load, v.get("REJECTION_TABLE"), v.get("REJECTION_SCHEMA"))
            log.log_message("Rejection table load  Completed " + str(datetime.datetime.now()))

            log.log_message("LOADING THE REJECTION TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_REJ_LOAD")

        if v.get("BOOKMARK") == "AFTER_REJ_LOAD":
            log.log_message("UPDATING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")
            update_tgt_table = f""" UPDATE {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} AS T1
                                                       SET T1.DESCRIPTION = T2.DESCRIPTION ,
                                                       T1.DEPARTMENT = T2.DEPARTMENT,
                                                       T1.CLASS = T2.CLASS,
                                                       RCD_UPDATE_DT = LOCALTIMESTAMP
                                                       FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } AS T2
                                                       WHERE T1.PROD_ID = T2.PROD_ID
                                                       AND
													   (
                                                        NVL(T1.DESCRIPTION, '?') <> NVL(T2.DESCRIPTION, '?')
                                                        OR NVL(T1.DEPARTMENT, '?') <> NVL(T2.DEPARTMENT, '?')
                                                        OR NVL(T1.CLASS, '?') <> NVL(T2.CLASS, '?')
													   );
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
                            PROD_ID,
                            DESCRIPTION,
                            DEPARTMENT,
                            CLASS,
                            CURRENT_FLG,
                            RCD_INSERT_DT,
                            RCD_UPDATE_DT
                        ) SELECT
                            PROD_ID,
                            DESCRIPTION,
                            DEPARTMENT,
                            CLASS,
                            'Y',
                            LOCALTIMESTAMP,
                            LOCALTIMESTAMP
                        FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") }
            WHERE  PROD_ID IS NOT NULL AND
            PROD_ID NOT IN (SELECT DISTINCT PROD_ID from {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} )"""

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

from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect
from lib.BatchMonitor import BatchMonitor

def price_load():
    v = Variables()
    v.set("SCRIPT_NAME", 'Price_ld')
    v.set("TABLE","PRICE")
    v.set("TEMP_TABLE","TMP_PRICE_F")
    v.set("REJECTION_TABLE","REJ_PRICE_F")
    v.set("TARGET_TABLE","DWH_PRICE_F")
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    monitor=BatchMonitor(sqls,v,log,'LOAD')
    try:
        monitor.start_audit()
        if v.get("BOOKMARK")=="NULL":
            print("Price Loading Started " + str(datetime.datetime.now()))
            log.log_message("Price Loading Started " + str(datetime.datetime.now()))

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
                            SELECT CU_KEY
                                ,SRC.CU_ID
                                ,PROD_KEY
                                ,SRC.PROD_ID
                                ,UNIT_PRICE
                                ,SRC.SUPPLIER_FLAG
                            FROM {v.get("DATABASE") }.{v.get("SOURCE_SCHEMA") }.{v.get("TABLE") } SRC
                            LEFT JOIN {v.get("DATABASE") }.{v.get("TARGET_SCHEMA") }.DWH_PRODUCT_D PROD ON (SRC.PROD_ID=PROD.PROD_ID)
                            LEFT JOIN {v.get("DATABASE") }.{v.get("TARGET_SCHEMA") }.DWH_COMPANY_D COMP ON (SRC.CU_ID=COMP.CU_ID)

                             """

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
                                    FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } 
                                    where (PROD_KEY is not null OR CU_KEY is not null)"""

            sqls.load_table(temp_load, v.get("REJECTION_TABLE"), v.get("REJECTION_SCHEMA"))
            log.log_message("Rejection table load  Completed " + str(datetime.datetime.now()))

            log.log_message("LOADING THE REJECTION TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            monitor.set_bookmark("AFTER_REJ_LOAD")

        if v.get("BOOKMARK") == "AFTER_REJ_LOAD":
            log.log_message("UPDATING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****STARTED******-----------")
            update_tgt_table = f""" UPDATE {v.get("DATABASE")}.{v.get("TARGET_SCHEMA")}.{v.get("TARGET_TABLE")} AS T1
                                                       SET T1.TO_DATE = CURRENT_DATE,
                                                        RCD_UPDATE_DT = LOCALTIMESTAMP
                                                       FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } AS T2
                                                       WHERE T1.CU_ID = T2.CU_ID
                                                        AND T1.PROD_ID=T2.PROD_ID
                                                        AND T1.TO_DATE=TO_DATE('9999-12-30')
                                                        AND T1.FROM_DATE<CURRENT_DATE
                                                        AND(
                                                        NVL(T1.UNIT_PRICE ,'0')<> NVL(T2.UNIT_PRICE,'0') 
                                                       OR NVL(T1.SUPPLIER_FLAG,'0') <> NVL(T2.SUPPLIER_FLAG,'0')
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
                            CU_KEY
                            ,CU_ID
                            ,PROD_KEY
                            ,PROD_ID
                            ,FROM_DATE
	                        ,TO_DATE
                            ,UNIT_PRICE
                            ,SUPPLIER_FLAG
                            ,RCD_INSERT_DT
                            ,RCD_UPDATE_DT
                        ) SELECT
                            CU_KEY
                            ,CU_ID
                            ,PROD_KEY
                            ,PROD_ID
                            ,CURRENT_DATE
	                        ,DATE('9999-12-30') DATES
                            ,UNIT_PRICE
                            ,SUPPLIER_FLAG
                            ,LOCALTIMESTAMP
                            ,LOCALTIMESTAMP
                        FROM {v.get ("DATABASE") }.{v.get ("TEMP_SCHEMA") }.{v.get ("TEMP_TABLE") } SRC
            WHERE  (PROD_KEY is not null AND CU_KEY is not null) AND
            NOT EXISTS (
                         Select ROW_KEY
                            FROM B2B_PLATFORM.TARGET.DWH_PRICE_F TGT where NVL(TGT.CU_ID,'0')=NVL(SRC.CU_ID,'0')
                            AND NVL(TGT.PROD_ID,'0')=NVL(SRC.PROD_ID,'0') AND TGT.TO_DATE=DATE('9999-12-30')
                            )"""

            sqls.load_table(load_tgt_table, v.get("TARGET_TABLE"), v.get("TARGET_SCHEMA"))

            log.log_message("INSERTING THE TARGET TABLE FROM TEMP TABLE")
            log.log_message("------------*****COMPLETED******-----------")
            print("Price Loading Finished " + str(datetime.datetime.now()))
            log.log_message("Price Loading Finished " + str(datetime.datetime.now()))
            monitor.audit_complete()

    except Exception as e:
        print("Price Loading Failed",e)
        log.log_message("Price Loading Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        monitor.script_error()
        raise e

    finally:
        sqls.end_connection()
        log.close()

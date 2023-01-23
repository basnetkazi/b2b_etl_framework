from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect

class BatchMonitor():
    def __init__(self, db: SnowFlakeConnect, var: Variables, log: Logger, job):
        self.db = db
        self.var = var
        self.log = log

        # Setting up variables such that extraction and loading scripts will have different values
        if (job=='EXTRACTION'):
            self.schema = self.var.get("SOURCE_SCHEMA")
            self.batch_log = self.var.get("SRC_BATCH_LOG")
            self.batch_config = self.var.get("SRC_SCRIPTS_CONFIG")
        elif(job=='LOAD'):
            self.schema = self.var.get("TARGET_SCHEMA")
            self.batch_log = self.var.get("DWH_BATCH_LOG")
            self.batch_config = self.var.get("DWH_SCRIPTS_CONFIG")
        else:
            self.log.log_message("#################### End Script: Error ####################")
            self.log.log_message("JOB type unidentified ")
            exit(10)

    def get_job_id(self):
        if not self.var.exists("JOB_ID"):
            self.log.log_message(
                "#################### Get JOB_ID ####################")
            query = """
                SELECT JOB_ID FROM """ + self.schema + "." + self.batch_config + """ WHERE 
                UPPER(JOB_NAME) = UPPER('""" + self.var.get("SCRIPT_NAME") + """')
                """
            job_id = self.db.get_value(query)
            if job_id:
                self.var.set("JOB_ID", job_id[0])
                self.log.log_message("JOB_ID: " + str(job_id[0]))
            else:
                self.log.log_message("#################### End Script: Error ####################")
                self.log.log_message("No entry in " + self.batch_config + " table")
                print("No entry in " + self.batch_config + " table")
                exit(10)

        return self.var.get("JOB_ID")

    def get_batch_id(self):
        self.log.log_message(
            "#################### Get BATCH_ID ####################")
        query = """
                SELECT COALESCE(MAX(BATCH_ID),0) FROM """ + self.schema + """.""" + self.batch_log

        batch_id = self.db.get_value(query)
        self.var.set("BATCH_ID", batch_id[0])
        return self.var.get("BATCH_ID")

    def get_last_status(self):
        self.log.log_message(
            "#################### Get Last Run Status ####################")
        query = """
                SELECT coalesce(max(BATCH_ID),0) BATCH_ID
                    , coalesce(max(CASE WHEN STATUS = 'COMPLETE' THEN -1
                                   WHEN STATUS = 'RESTART' THEN 1
                                   WHEN STATUS = 'STARTED' THEN 2
                                   ELSE 0 END), 0) STATUS
                    , coalesce(max(BOOKMARK), 'NULL') BOOKMARK
                FROM """ + self.schema + """.""" + self.batch_log + """
                WHERE JOB_ID = '""" + str(self.var.get("JOB_ID")) + """'
                    AND BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID),0) FROM 
                                    """ + self.schema + """.""" + self.batch_log + """)
                """

        status = self.db.get_value(query)
        self.var.set("BATCH_ID", status[0])
        self.log.log_message("LAST_BATCH_ID: " + str(status[0]))
        self.var.set("LAST_RUN_STATUS", status[1])
        self.log.log_message("LAST_RUN_STATUS: " + str(status[1]))
        self.var.set("LAST_RUN_BOOKMARK", status[2])
        self.log.log_message("LAST_RUN_BOOKMARK: " + str(status[2]))
        return status

    def set_bookmark(self, bookmark_value):
        self.log.log_message(
            "#################### Set BOOKMARK ####################")
        self.log.log_message("~~~~~~~~~~~~~~~~" + bookmark_value)
        self.log.log_message(
            "######################################################")

        query = """
                UPDATE """ + self.schema + """.""" + self.batch_log + """
                SET BOOKMARK = '""" + bookmark_value + """'
                WHERE BATCH_ID = '""" + str(self.var.get("BATCH_ID")) + """' 
                AND JOB_ID = '""" + str(self.var.get("JOB_ID")) + """'"""
        self.db.execute_query(query)
        self.var.set("BOOKMARK", bookmark_value)

    def get_last_bookmark(self):
        self.log.log_message(
            "#################### Get LAST_RUN_BOOKMARK ####################")
        query = """
                SELECT MIN(BOOKMARK) FROM """ + self.schema + """.""" + self.batch_log + """
                WHERE BATCH_ID = '""" + str(self.var.get("BATCH_ID")) + """' 
                AND JOB_ID = '""" + str(self.var.get("JOB_ID")) + """'"""

        last_run_bookmark = self.db.get_value(query)
        if last_run_bookmark[0]:
            self.var.set("LAST_RUN_BOOKMARK", last_run_bookmark[0])
            self.log.log_message("LAST_RUN_BOOKMARK: " + last_run_bookmark[0])
            return self.var.get("LAST_RUN_BOOKMARK")
        else:
            return "No Bookmarks Found"

    def start_audit(self):
        self.get_job_id()
        batch_id, last_run_status, last_run_bookmark = self.get_last_status()

        if last_run_status == 1:
            self.log.log_message(
                "################ Restarting the Script #################")
            query = """
                        UPDATE """ + self.schema + """.""" + self.batch_log + """
                        SET STATUS 
                        = 'RESTARTED'
                        WHERE BATCH_ID = '""" + str(self.var.get("BATCH_ID")) + """' 
                        AND JOB_ID = '""" + str(self.var.get("JOB_ID")) + """'"""

            self.db.execute_query(query)
            self.var.set("BATCH_ID", batch_id)
            self.log.log_message("Last run Bookmark: " + last_run_bookmark)
            self.var.set("BOOKMARK", last_run_bookmark)

        elif last_run_status == 0:
            self.get_batch_id()
            batch_id = str(self.var.get("BATCH_ID"))
            self.log.log_message("BATCH_ID: " + batch_id)
            self.var.set("BOOKMARK", "NULL")
            self.log.log_message("BOOKMARK: " + "NONE")

            self.log.log_message(
                "################ Starting the Script ################")
            query = """
                        INSERT INTO """ + self.schema + """.""" + self.batch_log + """  (
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
                           SELECT """ + str(batch_id) + """
                                 ,""" + str(self.var.get("JOB_ID")) + """
                                 ,'""" + self.var.get("SCRIPT_NAME") + """'
                                 ,CURRENT_DATE
                                 ,CURRENT_TIMESTAMP
                                 ,NULL
                                 ,'RUNNING'
                                 ,''
                                 ,'""" + self.var.get("BOOKMARK") + """' 
                                 ,'""" + self.var.get("LOG_PATH") + """'
                            FROM dual
                         """

            self.db.execute_query(query)

        elif last_run_status == -1:

            self.log.log_message(
                'Error::::Trying to re-run completed script in the batch again.')
            raise Exception(
                'Error::::Trying to re-run completed script in the batch again.')

        else:
            self.log.log_message(
                'Error::::Script not ready for re-run. Check status of last execution.')
            raise Exception(
                'Error::::Script not ready for re-run. Check status of last execution.')

    def audit_complete(self):
        self.var.set("BOOKMARK", "COMPLETE")
        self.log.log_message(
            "#################### End Script: Successful ####################")
        sql = """
            UPDATE """ + self.schema + """.""" + self.batch_log + """
            SET END_TMS = CURRENT_TIMESTAMP
                , STATUS = 'COMPLETE'
                , BOOKMARK = 'COMPLETE'
            WHERE BATCH_ID = """ + str(self.var.get("BATCH_ID")) + """ AND JOB_ID = """ + str(self.var.get("JOB_ID"))

        self.db.execute_query(sql)

    def script_error(self):
        self.log.log_message(
            "#################### End Script: Error ####################")
        query = """
            UPDATE """ + self.schema + """.""" + self.batch_log + """
            SET END_TMS =  CURRENT_TIMESTAMP
                , STATUS = 'ERROR'
                , BOOKMARK = '""" + self.var.get("BOOKMARK") + """'
                , ERROR_MESSAGE = 'SCRIPT FAILED, SEE LOG FOR DETAIL'
            WHERE BATCH_ID = """ + str(self.var.get("BATCH_ID")) + """ AND JOB_ID = """ + str(self.var.get("JOB_ID"))

        self.db.execute_query(query)
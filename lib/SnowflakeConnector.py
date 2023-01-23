##import constants
import snowflake.connector
import json
import logging
from lib.Variables import Variables
from lib.Logger import  Logger
v=Variables()

class SnowFlakeConnect():
    def __init__(self, v: Variables, log : Logger):
        self.log=log
        self.v= v
        self.db = None
        self.USER = v.get("USER")
        self.PASSWORD = v.get("PASSWORD")
        self.ACCOUNT = v.get("ACCOUNT")
        self.DATABASE =  v.get("DATABASE")
        self.SCHEMA = v.get("SCHEMA")
        self.DATA_MART= v.get("DATA_MART")
        self.ROLE = v.get("ROLE")
        self.SOURCE_SCHEMA = v.get("SOURCE_SCHEMA")
        self.FILE_FORMAT = v.get("FILE_FORMAT")
        self.INTERNAL_STAGE = v.get("ISTAGE")
        self.COMPRESSION_TYPE = v.get("COMPRESSION_OPTION")
        self.OVERWRITE_OPTION = v.get("OVERWRITE_FLAG")
        self.STAGE_SCHEMA = v.get("STAGE_SCHEMA")

    def startConnection(self):
        ctx = snowflake.connector.connect(
            user= self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT,
            database=self.DATABASE,
            schema=self.SCHEMA
            )
        self.db=ctx.cursor()
        self.log.log_message("Database Session Started")
        self.execute_query(f"USE ROLE {self.ROLE}")
        self.execute_query(f"USE WAREHOUSE {self.DATA_MART}")


    def end_connection(self):
        self.db.close()
        self.log.log_message("Database Session Closed")


    def execute_query(self, query):
        try:
            self.db.execute(query)
            self.log.log_message(f"query executed: {query}")
            self.log.log_message("Snowflake Query ID :" + str(self.db.sfqid))
            self.log.log_message("Number of rows:" + str(self.db.rowcount))
            return self.db.rowcount
        except Exception as e:
            self.log.log_message(f"query error: {query}")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def get_value(self, query):
        try:
            self.db.execute(query)
            self.log.log_message(f"query executed: {query}")
            self.log.log_message("Snowflake Query ID :" + str(self.db.sfqid))
            self.log.log_message("Number of rows:" + str(self.db.rowcount))
            return self.db.fetchone()
        except Exception as e:
            self.log.log_message(f"query error: {query}")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def get_data(self, query):
        try:
            self.db.execute(query)
            self.log.log_message(f"query executed: {query}")
            self.log.log_message("Snowflake Query ID :" + str(self.db.sfqid))
            self.log.log_message("Number of rows:" + str(self.db.rowcount))
            return self.db.fetchall()
        except Exception as e:
            self.log.log_message(f"query error: {query}")
            self.log.log_message(f"ERROR: {e}")
            raise e


    def truncate_table(self, database, schema,  table):
        try:
            truncate_query = f"""TRUNCATE TABLE {database}.{schema}.{table}"""
            self.execute_query(truncate_query)
            self.log.log_message(f"Successfully truncated {schema}.{table}")
        except Exception as e:
            print(f"Failed to truncate {schema}.{table}",e)
            self.log.log_message(f"Failed to truncate table {schema}.{table}")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def create_data_file(self, table):
        try:
            load_istg_query = f"""copy into @{self.INTERNAL_STAGE}/DATA_{table} from {self.DATABASE}.{self.SOURCE_SCHEMA}.{table} 
                                file_format = (format_name = {self.FILE_FORMAT} compression = {self.COMPRESSION_TYPE}) overwrite = {self.OVERWRITE_OPTION}; """

            result = self.get_value(load_istg_query)
            print(f"Sucessfully created data file for {table}")
            self.log.log_message(f"Sucessfully created data file for {table}")
            return result
        except Exception as e:
            print(f"Failed to created data file for {table}",e)
            self.log.log_message(f"Failed to created data file for {table} ")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def create_data_file_delta(self, table):
        try:
            load_istg_query = f"""copy into @{self.INTERNAL_STAGE}/DATA_{table} from (Select * from {self.DATABASE}.{self.SOURCE_SCHEMA}.{table} where LAST_UPDATE_DT>DATE(sysdate())-2)
                                file_format = (format_name = {self.FILE_FORMAT} compression = {self.COMPRESSION_TYPE}) overwrite = {self.OVERWRITE_OPTION}; """

            result=self.get_value(load_istg_query)
            print(f"Sucessfully created data file for {table}")
            self.log.log_message(f"Sucessfully created data file for {table}")
            return result
        except Exception as e:
            print(f"Failed to created data file for {table}",e)
            self.log.log_message(f"Failed to created data file for {table} ")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def load_stage_from_file(self, table):
        try:
            self.truncate_table(self.DATABASE,self.STAGE_SCHEMA,'STG_'+table)
            fill_stg_query = f""" COPY INTO  {self.DATABASE}.{self.STAGE_SCHEMA}.STG_{table} from @{self.INTERNAL_STAGE}/DATA_{table}
                                    file_format = (format_name = {self.FILE_FORMAT} compression = {self.COMPRESSION_TYPE}); """
            result = self.get_value(fill_stg_query)
            print(f"Sucessfully loaded from data file to stage table STG_{table}")
            self.log.log_message(f"Sucessfully loaded from data file  to stage table STG_{table}")
            return result

        except Exception as e:
            print(f"Failed to load from data file to stage tables {table}",e)
            self.log.log_message(f"Failed to load data file to stage tables {table}")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def load_stage_from_log(self, table):
        try:
            self.truncate_table(self.DATABASE, self.STAGE_SCHEMA, table)
            fill_stg_query = f""" COPY INTO  {self.DATABASE}.{self.STAGE_SCHEMA}.{table} from @{v.get("SOURCE_SCHEMA")}.{self.INTERNAL_STAGE}
                                    file_format = (format_name = {v.get("STAGE_SCHEMA")}.LOG_LOAD  compression = {self.COMPRESSION_TYPE}) pattern = '.*.log' ON_ERROR = 'CONTINUE'; """
            result = self.get_value(fill_stg_query)
            print(f"Sucessfully loaded from data file to stage table STG_{table}")
            self.log.log_message(f"Sucessfully loaded from data file  to stage table STG_{table}")
            return result

        except Exception as e:
            print(f"Failed to load from data file to stage tables {table}", e)
            self.log.log_message(f"Failed to load data file to stage tables {table}")
            self.log.log_message(f"ERROR: {e}")
            raise e

    def load_table(self, query, table, schema):
        try:
            result= self.execute_query(query)
            print(f"Successfully loaded to {schema} table {table}")
            self.log.log_message(f"Successfully loaded to {schema} table {table}")
            return result
        except Exception as e:
            print(f"Failed to load {schema} table {table}",e)
            self.log.log_message(f"Failed to load {schema} table {table} ")
            self.log.log_message(f"ERROR: {e}")
            raise e



from lib.Variables import Variables
import datetime
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect




def create_file_format():
    v = Variables()
    v.set("SCRIPT_NAME", 'File_format_create')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    format = v.get("FILE_FORMAT")
    print("File Formate creation started")
    log.log_message("File Formate creation started")
    try:
        sql = f""" 
        create or replace file format {v.get("SOURCE_SCHEMA")}.{format}
                type = csv
                field_delimiter = '|'
                field_optionally_enclosed_by = '0x27'
                null_if = ('NULL');
        """

        sqls.execute_query(sql)
        print("Sucessfully created a file format ", format)
        log.log_message(f"Sucessfully created a file format {format}")

        sql = f""" 
                create or replace file format {v.get("STAGE_SCHEMA")}.LOG_LOAD
                                    FIELD_DELIMITER = ' '
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            ;
                """

        sqls.execute_query(sql)
        print("Sucessfully created a file format ", format)
        log.log_message(f"Sucessfully created a file format {format}")

    except Exception as e:
        print("Failed to create File Format",e)
        log.log_message(f"Failed to create File Format {e}")
        log.log_message(f"ERROR: {e}")
        raise e

    finally:
        sqls.end_connection()
        log.close()
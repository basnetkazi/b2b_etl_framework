from lib.Variables import Variables
import datetime
from os import path


class Logger:
    def __init__(self, v:Variables):
        self.v=v
        log_path = v.get("LOG_PATH")
        script_name = v.get("SCRIPT_NAME")
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name = str(script_name) + "_" + current_time + ".log"
        log_file = path.join(log_path, log_file_name)
        self.log_file = open(log_file, 'w')
        #v.set("LOG_FILE", log_file_name)
        #v.set("LOG_CUR_DATETIME", current_time)

    def log_message(self, msg):
        now = datetime.datetime.now()
        msg = msg
        # print(msg)
        self.log_file.write(str(now))
        self.log_file.write(": ")
        self.log_file.write(msg)
        self.log_file.write("\n")
        self.log_file.flush()

    def close(self):
        self.log_file.close()
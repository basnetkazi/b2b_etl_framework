from lib.Variables import Variables
import datetime
import random
import shutil, ntpath
import os
from os import path
from faker import Faker
from fake_useragent import UserAgent
from lib.Logger import Logger
from lib.SnowflakeConnector import SnowFlakeConnect

def random_date(start, end):
    start=datetime.datetime.strptime(start, '%x')
    end=datetime.datetime.strptime(end, '%x')
    delta = end-start
    delta_sec=delta.days*60*20*20+delta.seconds
    rand_Sec= random.randrange(delta_sec)
    return (start+datetime.timedelta(seconds=rand_Sec)).strftime('[%d/%b/%Y:%H:%M:%S +0545]')


def log_generator():
    v = Variables()
    v.set("SCRIPT_NAME", 'web_log_generator')
    log = Logger(v)
    sqls = SnowFlakeConnect(v, log)
    sqls.startConnection()
    faker=Faker()
    ua = UserAgent()
    status=('200','404','302','301','405','304','406','500','-')


    try:
        log_path = v.get("WEBLOG")
        files= os.listdir(log_path)
        print("Log Archive Started " + str(datetime.datetime.now()))
        log.log_message("Log Archive Started " + str(datetime.datetime.now()))
        for f in files:
            f_arch = str(datetime.date.today()) + """_""" + ntpath.basename(f)
            shutil.move(log_path+'/' + f, v.get("WEB_ARCHIVE")+ "/" + f_arch)

        print("Log Archive Completed " + str(datetime.datetime.now()))
        log.log_message("Log Archive Completed " + str(datetime.datetime.now()) +"to path "+v.get("WEB_ARCHIVE"))
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name = "weblog_" + current_time + ".log"
        log_file = path.join(log_path, log_file_name)
        log_file = open(log_file, 'w')
        print("Log Creation Started " + str(datetime.datetime.now()))
        log.log_message("Log Creation Started " + str(datetime.datetime.now()))
        #create stage file for company table
        username = sqls.get_data(
            "Select distinct login_name from SOURCE.COMPANY UNION ALL Select distinct login_name from SOURCE.CUSTOMER order by RANDOM()")

        for i in range(50000):
            date=random_date('1/1/21','7/4/22')
            log_file.write(faker.ipv4()+' - '+random.choice(username)[0]+' '+date+' - '+ random.choice(status) +' '+ str(random.randrange(9944)) + ' - "'+ua.random+'"')
            log_file.write("\n")
            log_file.flush()
            # print(random.choice(username)[0])
            # print(faker.ipv4())
            # print(ua.random)
            # print(random_date('1/1/21','7/4/22'))
        log_file.close()
        print("Log CreationFinished " + str(datetime.datetime.now()))
        log.log_message("Log Creation Finished " + str(datetime.datetime.now())+"to path "+v.get("WEBLOG"))


    except Exception as e:
        print("Log Creation Failed",e)
        log.log_message("Log Creation Failed " + str(datetime.datetime.now()))
        log.log_message(f"ERROR: {e}")
        raise e
    finally:
        sqls.end_connection()
        log.close()

log_generator()
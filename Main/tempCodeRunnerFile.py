import Main
import time
import datetime
from Postgresl_Connection import database_server

def schedule_running_time(run):
    try:
        print("Running Date_config task")
        run = database_server()



    except Exception as error:
        print(error)
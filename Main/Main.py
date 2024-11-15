import json
import schedule
import time
from ExcelConversion.ExcelToJson import run_task
from Postgresql.database_server import Postgres


# This will load the config for date time loop, the configurations needs to be done into the Json archive
def loadconfig():
    with open("Main/DateScheduleConfig/Schedule_config.json") as config_file:
        config = json.load(config_file)
    # Validate configuration settings
    interval_type = config.get("interval_type", "weekly").lower()
    time_of_day = config.get("time", "21:00")
    day_of_week = config.get("day_of_week", "friday").lower()

    return {
        "interval_type": interval_type,
        "time": time_of_day,
        "day_of_week": day_of_week
    }


def setup_schedule():
    config = loadconfig()
    interval_type = config["interval_type"]
    time_of_day = config["time"]
    # Changes the configurations of Schedule based on the Json file
    if interval_type == "hourly":
        schedule.every().hour.do(main)
    elif interval_type == "daily":
        schedule.every().day.at(time_of_day).do(main)
    elif interval_type == "weekly":
        day_of_week = config["day_of_week"]
        if hasattr(schedule.every(), day_of_week):
            getattr(schedule.every(), day_of_week).at(time_of_day).do(main)
        else:
            print("Invalid day of week in config.")
    elif interval_type == "minute":  # Optional for testing purposes
        schedule.every(1).minutes.do(main)
    else:
        print("Invalid interval type in config.")


#
def main(db=None):
    try:
        # Instantiate and connect to the database if not passed
        if db is None:
            try:
                # Specify the database parameters
                db = Postgres("teste", "postgres", "159753")
                db.connect()
            except ConnectionError as e:
                print(f"Connection not possible: {e}")

        print("Connection established with DATABASE")
        run_task()
        db.insert_table()
    except ConnectionError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"Error running scheduled task: {e}")
    finally:
        db.close()  # Ensure the database connection is closed after task execution


setup_schedule()
print("Scheduler is running...")
while True:
    schedule.run_pending()
    time.sleep(1)

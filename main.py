from catchment_data import Catchment
from db_controller import DBController
import os
import csv
import datetime
import time


GAGE_FILE = os.path.join(".", "data", "camels-gages.csv")
EXCEPTION_TIMEOUT = 60


def execute(n=0):
    gages = load_gages()
    print("All gauges: {}".format(len(gages)))
    db = DBController()
    todo_gages = []
    for g in gages:
        status = db.data_check(g["SOURCE_FEA"], g["FLComID"])
        if not all(status):
            todo_gages.append(g)
    todo_n = len(todo_gages)
    if todo_n == 0:
        print("Completed downloading data for all {} gauge catchments".format(len(gages)))
        return
    print("Completed gauges: {}".format(len(gages) - todo_n))
    print("TODO gauges: {}".format(todo_n))
    print("Cycle: {}".format(n))
    i = 1
    for g in todo_gages:
        t0 = time.time()
        comid = g["FLComID"]
        gageid = g["SOURCE_FEA"]
        print("Retrieving data for COMID: {}, GAGEID: {}".format(comid, gageid))
        print("# in session: {}, # remaining: {}".format(i, todo_n + 1 - i))
        print("Execution time: {}".format(datetime.datetime.now()))
        try:
            catchment = get_data(gageid, comid)
        except Exception as e:
            print("Error attempting to download data for COMID: {}, GAGEID: {}".format(comid, gageid))
            print("Error: {}".format(e))
            print("Waiting {} seconds before continuing".format(EXCEPTION_TIMEOUT))
            time.sleep(EXCEPTION_TIMEOUT)
            continue
        t1 = time.time()
        try:
            db.save(catchment, close=False)
        except Exception as e:
            print("Error attempting to save data for COMID: {}, GAGEID: {}".format(comid, gageid))
            print("Error: {}".format(e))
            print("Waiting {} seconds before continuing".format(EXCEPTION_TIMEOUT))
            time.sleep(EXCEPTION_TIMEOUT)
            continue
        t2 = time.time()
        print("Catchment Data saved for COMID: {}, GAGEID: {}".format(comid, gageid))
        print("Runtime: {} sec".format(round(t2 - t0, 4)))
        print("Data Retrieval time: {} sec".format(round(t1 - t0, 4)))
        print("DB Processing time: {} sec".format(round(t2 - t1, 4)))
        i += 1
    db.close()
    execute(n+1)


def load_gages():
    with open(GAGE_FILE, newline='') as csvfile:
        gage_reader = csv.DictReader(csvfile)
        data = []
        for r in gage_reader:
            data.append(dict(r))
    return data


def get_data(gageid, comid):
    catchment = Catchment(gage=gageid, comid=comid)
    catchment.get_data_parallel()
    catchment.assemble()
    catchment.get_metrics()
    return catchment


if __name__ == '__main__':
    # TODO: Add error handling and data checking
    t0 = time.time()
    execute()
    t1 = time.time()
    print("Total Runtime: {} sec".format(round(t1-t0, 4)))

from catchment_data import Catchment
from db_controller import DBController
import os
import csv
import time


GAGE_FILE = os.path.join(".", "data", "camels-gages.csv")


def execute():
    gages = load_gages()
    print("All gauges: {}".format(len(gages)))
    db = DBController()
    todo_gages = []
    for g in gages:
        status = db.data_check(g["SOURCE_FEA"], g["FLComID"])
        if not all(status):
            todo_gages.append(g)
    print("Completed gauges: {}".format(len(gages) - len(todo_gages)))
    print("TODO gauges: {}".format(len(todo_gages)))
    for g in todo_gages:
        t0 = time.time()
        comid = g["FLComID"]
        gageid = g["SOURCE_FEA"]
        print("Retrieving data for COMID: {}, GAGEID: {}".format(comid, gageid))
        catchment = get_data(gageid, comid)
        t1 = time.time()
        db.save(catchment)
        t2 = time.time()
        print("Catchment Data saved for COMID: {}, GAGEID: {}".format(comid, gageid))
        print("Runtime: {} sec".format(round(t2 - t0, 4)))
        print("Data Retrieval time: {} sec".format(round(t1 - t0, 4)))
        print("DB Processing time: {} sec".format(round(t2 - t1, 4)))
        return
    db.close()


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

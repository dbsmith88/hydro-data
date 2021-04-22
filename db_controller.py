import sqlite3
from config import Configs


class DBController:

    TIMEOUT = 1.0
    I_MAX = 400

    CATCHMENTDATA_COUNT = 99
    FORCINGDATA_COUNT = 271751

    def __init__(self):
        self.connected = False
        self.conn = self.connect()

    def connect(self):
        self.connected = True
        return sqlite3.connect(Configs.DB_PATH, timeout=self.TIMEOUT)

    def close(self):
        self.connected = False
        self.conn.close()

    def save(self, catchment, close=True):
        if not self.connected:
            self.conn = self.connect()
        self.save_forcing(catchment)
        self.save_streamflow(catchment)
        self.save_catchment_data(catchment)
        # precip metadata
        self.save_metadata(catchment.precipitation["metadata"], "prcp", catchment.comid)
        # daymet metadata
        self.save_metadata(catchment.daymet["metadata"], "daymet", catchment.comid)
        # temperature metadata
        self.save_metadata(catchment.temperature["metadata"], "temp", catchment.comid)
        # radiation metadata
        self.save_metadata(catchment.radiation["metadata"], "rad", catchment.comid)
        # evapotranspiration metadata
        self.save_metadata(catchment.evapotranspiration["metadata"], "evapo", catchment.comid)
        if close:
            self.close()

    def save_forcing(self, catchment):
        c = self.conn.cursor()
        c.execute("BEGIN")
        for index, row in catchment.df.iterrows():
            values = (
                catchment.gageid, row.year, row.month, row.day, row.hour, row.dayl, row.prcp, row.swrad, row.lwrad,
                row.swe, row.temp, None, row.vp, row.et,
            )
            query = "INSERT OR REPLACE INTO " \
                    "ForcingData(gageID,year,mnth,day,hr,dayl,prcp,srad,lrad,swe,tmax,tmin,vp,et) " \
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            c.execute(query, values)
            if index % self.I_MAX == 0:
                c.execute("COMMIT")
                c.execute("BEGIN")
        c.execute("COMMIT")
        c.close()

    def save_metadata(self, metadata, dataset, comid):
        c = self.conn.cursor()
        c.execute("BEGIN")
        for k, v in metadata.items():
            values = (comid, dataset, k, v)
            query = "INSERT OR REPLACE INTO ForcingMetadata(COMID,dataset,key,value) VALUES(?,?,?,?)"
            c.execute(query, values)
        c.execute("COMMIT")
        c.close()

    def save_streamflow(self, catchment):
        # TODO: Add check for streamflow source, if HMS iterate through catchment.df for q
        c = self.conn.cursor()
        c.execute("BEGIN")
        for index, row in catchment.streamflow.iterrows():
            values = (catchment.gageid, row.year, row.month, row.day, None, row.q)
            query = "INSERT OR REPLACE INTO StreamflowData(gageID,year,mnth,day,hr,streamflow) VALUES(?,?,?,?,?,?)"
            c.execute(query, values)
            if index % self.I_MAX == 0:
                c.execute("COMMIT")
                c.execute("BEGIN")
        c.execute("COMMIT")
        c.close()

    def save_catchment_data(self, catchment):
        c = self.conn.cursor()
        c.execute("BEGIN")
        query = "INSERT OR REPLACE INTO CatchmentGage(COMID, gageID) VALUES(?,?)"
        values = (catchment.comid, catchment.gageid,)
        c.execute(query, values)
        query = "INSERT OR REPLACE INTO CatchmentData(COMID, parameter, value) VALUES(?,?,?)"
        for k, v in catchment.catchment["metadata"].items():
            if k is not "ComID":
                values = (catchment.comid, k, v)
                c.execute(query, values)

        sandcat = None
        claycat = None
        query = "INSERT OR REPLACE INTO CatchmentData(COMID, parameter, value) VALUES(?,?,?)"
        for m in catchment.catchment["streamcat"]["metrics"]:
            if m["id"] in catchment.characteristics:
                values = (catchment.comid, m["id"], m["metric_value"])
                c.execute(query, values)
                if m["id"] == "sandcat":
                    sandcat = m["metric_value"]
                if m["id"] == "claycat":
                    claycat = m["metric_value"]
        siltcat = 100.0 - sandcat - claycat
        values = (catchment.comid, "siltcat", siltcat)
        c.execute(query, values)
        for k, v in catchment.metrics.items():
            values = (catchment.comid, k, v)
            c.execute(query, values)
        c.execute("COMMIT")
        c.close()

    def data_check(self, gageid, comid):
        results = []
        c = self.conn.cursor()

        query = "SELECT COUNT() FROM CatchmentData WHERE COMID=?"
        n = c.execute(query, (comid,)).fetchone()[0]
        if n == self.CATCHMENTDATA_COUNT:
            results.append(True)
        else:
            results.append(False)

        query = "SELECT COUNT() FROM ForcingData WHERE gageID=?"
        n = c.execute(query, (gageid,)).fetchone()[0]
        if n == self.FORCINGDATA_COUNT:
            results.append(True)
        else:
            results.append(False)

        query = "SELECT COUNT() FROM StreamflowData WHERE gageID=?"
        n = c.execute(query, (gageid,)).fetchone()[0]
        if n > 0:
            results.append(True)
        else:
            results.append(False)
        return results

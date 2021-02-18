from hms_data import HMS
from config import Configs
import time
import multiprocessing as mp


class Catchment:

    characteristics = [
        "pctconif2011cat", "pctdecid2011cat", "pctbl2011cat", "pctcrop2011cat", "pctgrs2011cat", "pcthay2011cat",
        "pcthbwet2011cat", "pctice2011cat", "pctmxfst2011cat", "pctow2011cat", "pctshrb2011cat", "pcturbhi2011cat",
        "pcturblo2011cat", "pcturbmd2011cat", "pcturbop2011cat", "pctwdwet2011cat", "runoffcat", "precip8110cat",
        "tmax8110cat", "tmin8110cat", "omcat", "wtdepcat", "permcat", "pctcarbresidcat", "claycat", "sandcat",
        "hydrlcondcat", "avgwetindxcat", "rckdepcat"
    ]

    def __init__(self, gage, comid):
        self.start_date = Configs.START_DATE.strftime("%m-%d-%Y")
        self.end_date = Configs.END_DATE.strftime("%m-%d-%Y")
        self.gageid = str(gage)
        self.comid = str(comid)
        self.precipitation = None
        self.streamflow = None
        self.streamSource = None
        self.daymet = None
        self.temperature = None
        self.radiation = None
        self.evapotranspiration = None
        self.catchment = None
        self.df = None
        self.metrics = None
        self.completed = False

    def get_data(self):
        self.get_precipitiation()
        self.get_streamflow()
        self.get_daymet()
        self.get_temperature()
        self.get_radiation()
        self.get_evapotranspiration()
        self.get_catchment_data()
        self.is_completed()

    def get_data_parallel(self):
        pool = mp.Pool(mp.cpu_count())
        datasets = ["precipitation", "streamflow", "daymet", "temperature", "radiation", "evapotranspiration", "catchment"]
        pool_args = [(self, d) for d in datasets]
        results = pool.starmap_async(self.get_dataset, [a for a in pool_args]).get()
        pool.close()
        for r in results:
            if r is not None:
                setattr(self, r["dataset"], r["data"])
        self.is_completed()

    @staticmethod
    def get_dataset(self, dataset):
        if dataset == "precipitation":
            return self.get_precipitiation(return_data=True)
        elif dataset == "streamflow":
            return self.get_streamflow(return_data=True)
        elif dataset == "daymet":
            return self.get_daymet(return_data=True)
        elif dataset == "temperature":
            return self.get_temperature(return_data=True)
        elif dataset == "radiation":
            return self.get_radiation(return_data=True)
        elif dataset == "evapotranspiration":
            return self.get_evapotranspiration(return_data=True)
        elif dataset == "catchment":
            return self.get_catchment_data(return_data=True)

    def is_completed(self):
        if self.precipitation is not None and self.streamflow is not None and self.daymet is not None and self.temperature is not None and self.radiation is not None and self.evapotranspiration is not None and self.catchment is not None:
            self.completed = True

    def get_precipitiation(self, return_data=False):
        precip = HMS(self.start_date, self.end_date, Configs.PRECIP_SOURCE, "Precipitation", "Meteorology", Configs.TIMESTEP)
        precip.set_geometry("comid", self.comid)
        precip.submit_request()
        if return_data:
            return { "dataset": "precipitation", "data": precip.data}
        self.precipitation = precip.data

    def get_streamflow(self, hms=False, return_data=False):
        if hms:
            self.streamSource = "HMS"
            streamflow = HMS(self.start_date, self.end_date, "nwis", "Streamflow", "Hydrology", Configs.TIMESTEP)
            streamflow.set_geometry(metadata={"gaugestation": self.gageid})
            streamflow.submit_request()
            if return_data:
                return { "dataset": "streamflow", "data": streamflow.data}
            self.streamflow = streamflow.data
        else:
            self.streamSource = "FILE"
            if return_data:
                return { "dataset": "streamflow", "data": Configs().load_from_file(self.gageid)}
            self.streamflow = Configs().load_from_file(self.gageid)

    def get_solar(self, return_data=False):
        solar = HMS(self.start_date, self.end_date, None, "Solar", "Meteorology", "60")
        solar.variables["model"] = "Daily"
        solar.set_geometry("comid", self.comid)
        solar.submit_request()
        if return_data:
            return { "dataset": "solar", "data": solar.data }
        self.solar = solar.data

    def get_daymet(self, return_data=False):
        daymet = HMS(self.start_date, self.end_date, "daymet", "Precipitation", "Meteorology", "daily")
        daymet.set_geometry("comid", self.comid, {"vars": "vp,swe,dayl,prcp"})
        daymet.submit_request()
        if return_data:
            return { "dataset": "daymet", "data": daymet.data }
        self.daymet = daymet.data

    def get_temperature(self, return_data=False):
        temp = HMS(self.start_date, self.end_date, Configs.TEMP_SOURCE, "Temperature", "Hydrology", Configs.TIMESTEP)
        temp.set_geometry("comid", self.comid)
        temp.submit_request()
        if return_data:
            return { "dataset": "temperature", "data": temp.data }
        self.temperature = temp.data

    def get_radiation(self, return_data=False):
        rad = HMS(self.start_date, self.end_date, Configs.RAD_SOURCE, "Radiation", "Meteorology", Configs.TIMESTEP)
        rad.set_geometry("comid", self.comid)
        rad.submit_request()
        if return_data:
            return { "dataset": "radiation", "data": rad.data }
        self.radiation = rad.data

    def get_evapotranspiration(self, return_data=False):
        evapo = HMS(self.start_date, self.end_date, Configs.ET_SOURCE, "Evapotranspiration", "Hydrology", Configs.TIMESTEP)
        evapo.variables = {"algorithm": "nldas"}
        evapo.set_geometry("comid", self.comid)
        evapo.submit_request()
        if return_data:
            return { "dataset": "evapotranspiration", "data": evapo.data }
        self.evapotranspiration = evapo.data

    def get_catchment_data(self, return_data=False):
        if return_data:
            return {"dataset": "catchment", "data":  HMS.get_info(self.comid)}
        self.catchment = HMS.get_info(self.comid)

    def assemble(self):
        if not self.completed:
            print("Data retrieval not complete for gage: {}, comid: {}".format(self.gageid, self.comid))
            return
        df = Configs().initialize_df()
        df = Configs().insert_timeseries(df, ["prcp"], self.precipitation["data"])
        df = Configs().insert_timeseries(df, ["vp", "swe", "dayl", "dprcp"], self.daymet["data"], interpolate="bfill")
        df = Configs().insert_timeseries(df, ["temp"], self.temperature["data"])
        df = Configs().insert_timeseries(df, ["lwrad", "swrad"], self.radiation["data"])
        df = Configs().insert_timeseries(df, ["et"], self.evapotranspiration["data"])
        #TODO: Add check for stream source, if HMS add to df
        # df = Configs().insert_timeseries(df, ["q"], self.streamflow["data"], interpolate='gaussian', plot=True, title="Streamflow")
        self.df = df

    def get_metrics(self):
        prcp_mean = self.df["prcp"].mean()
        et_mean = self.df["et"].mean()
        prcp_tot = self.df["prcp"].sum()
        prcp_daymet_tot = self.df["dprcp"].sum()
        swe_tot = self.df["swe"].sum()
        frac_snow = swe_tot / prcp_daymet_tot
        aridity = et_mean / prcp_mean

        i_year = Configs.START_DATE.year
        h_p_yearly = []
        h_p_c_yearly = []
        l_p_yearly = []
        l_p_c_yearly = []
        while i_year <= Configs.END_DATE.year:
            p_year = self.df[(self.df["year"] == i_year)]
            h = p_year[(p_year["prcp"] >= prcp_mean * 5.0)]
            h_p = h["prcp"].count()
            h_p_c = [1]
            for i in range(1, h.index.size):
                if h.index[i] == h.index[i-1] + 1:
                    h_p_c.append(h_p_c[i-1] + 1)
                else:
                    h_p_c.append(1)
            l = p_year[(p_year["prcp"] > 0.0)]
            l = l[1.0 / 24.0 > l["prcp"]]
            l_p = l["prcp"].count()                                     # 1/24 mm an hour => 1mm/day
            l_p_c = [1]
            for i in range(1, l.index.size):
                if l.index[i] == l.index[i-1] + 1:
                    l_p_c.append(l_p_c[i-1] + 1)
                else:
                    l_p_c.append(1)
            h_p_yearly.append(h_p)
            h_p_c_yearly.append(int(sum(h_p_c)/len(h_p_c)))
            l_p_yearly.append(l_p)
            l_p_c_yearly.append(int(sum(l_p_c)/len(l_p_c)))
            i_year = i_year + 1
        hp = int(sum(h_p_yearly) / len(h_p_yearly))
        hpc = int(sum(h_p_c_yearly) / len(h_p_c_yearly))
        lp = int(sum(l_p_yearly) / len(l_p_yearly))
        lpc = int(sum(l_p_c_yearly) / len(l_p_c_yearly))

        self.metrics = {
            "prcp_mean": round(prcp_mean,4),
            "prcp_tot": round(prcp_tot, 4),
            "hi_prcp_f": hp,
            "hi_prcp_d": hpc,
            "low_prcp_f": lp,
            "low_prcp_d": lpc,
            "aridity": round(aridity, 4),
            "frac_snow": round(frac_snow, 4)
        }


if __name__ == "__main__":
    comid = "6177558"
    gageid = "1195100"
    t0 = time.time()
    c = Catchment(gage=gageid, comid=comid)
    c.get_data()
    t1 = time.time()
    c.assemble()
    c.get_metrics()
    t2 = time.time()
    print("Runtime: {} sec".format(round(t2-t0, 4)))
    print("Data Retrieval time: {} sec".format(round(t1-t0, 4)))
    print("Post-Processing time: {} sec".format(round(t2-t1, 4)))

import os
import json
import requests
import time
import logging
from config import Configs


logger = logging.getLogger(__name__)



class HMS:

    def __init__(self, start_date=None, end_date=None, source=None, dataset=None, module=None, ts="hourly"):
        self.start_date = start_date
        self.end_date = end_date
        self.source = source
        self.dataset = dataset
        self.module = module
        self.timestep = ts
        self.geometry = {
            "geometryMetadata": {}
        }
        self.task_id = None
        self.task_status = None
        self.data = None
        self.cookies = Configs.HMS_COOKIE
        self.comid = None
        self.metadata = None
        self.variables = {}

    def print_info(self):
        if not self.data:
            print("Unable to retrieve data for: {}".format(json.dumps(self.geometry)))
            return
        data = json.loads(self.data)
        dates = list(data["data"].keys())
        l = len(data["data"])
        gtype = "comid"
        gvalue = self.comid
        if "gaugestation" in self.geometry["geometryMetadata"].keys():
            gtype = "gaugestation"
            gvalue = self.geometry["geometryMetadata"]["gaugestation"]
        print("Dataset: {}, {}: {}, Source: {}, Status: {}".format(self.dataset, gtype, gvalue, self.source, self.task_status))
        if l > 0:
            print("Length: {}, Start-Date: {}, End-Date: {}".format(len(data["data"]), dates[0], dates[-1]))
        else:
            print("Length: {}, Start-Date: {}, End-Date: {}".format(0, "NA", "NA"))

    def set_geometry(self, gtype="point", value=None, metadata=None):
        if value:
            # Valid gtypes: COMID, HUCID, STATIONID, POINT with value={"latitude": LAT, "longitude": LNG"}
            self.geometry[gtype] = value
        if metadata:
            self.geometry["geometryMetadata"] = metadata

    def get_request_body(self):
        if any((self.dataset, self.source, self.start_date, self.end_date, self.geometry, self.module)) is None:
            logger.info("Missing required parameters, unable to create request.")
            return None
        request_body = {
            "source": self.source,
            "dateTimeSpan": {
                "startDate": self.start_date,
                "endDate": self.end_date
            },
            "geometry": self.geometry,
            "temporalResolution": self.timestep
        }
        for k, v in self.variables.items():
            request_body[k] = v
        return request_body

    def submit_request(self):
        params = json.dumps(self.get_request_body())
        if params is None:
            self.task_status = "FAILED: Parameters invalid"
            return None
        request_url = Configs.HMS_BASE_URL + self.module + "/" + self.dataset + "/"
        header = {"Referer": request_url}
        logger.info("Submitting data request.")
        try:
            response_txt = requests.post(request_url, data=params, cookies=self.cookies, headers=header).text
        except ConnectionError as error:
            self.task_status = "FAILED: Failed Request"
            logger.info("WARNING: Failed data request")
            return None
        response_json = json.loads(response_txt)
        self.task_id = response_json["job_id"]
        self.task_status = "SENT"
        self.get_data()

    @staticmethod
    def get_info(comid):
        info_url = Configs.HMS_INFO_URL + comid + "&streamcat=true"
        try:
            response_txt = requests.get(info_url).text
        except ConnectionError as error:
            logger.info("WARNING: Unable to get catchment info for: {}. Error: {}".format(comid, error))
            return
        response_json = json.loads(response_txt)
        return response_json

    def get_data(self):
        if self.task_id is None:
            logger.info("WARNING: No task id")
            self.task_status = "FAILED: No task id"
            return None
        time.sleep(5)
        retry = 0
        n_retries = 100
        data_url = Configs.HMS_DATA_URL + self.task_id
        success_fail = False
        while retry < n_retries and not success_fail:
            response_txt = requests.get(data_url, cookies=self.cookies).text
            response_json = json.loads(response_txt)
            self.task_status = response_json["status"]
            if self.task_status == "SUCCESS":
                if type(response_json["data"]) is dict:
                    self.data = response_json["data"]
                else:
                    try:
                        self.data = json.loads(response_json["data"])
                        success_fail = True
                    except Exception:
                        success_fail = True
                        print("Failure: {}".format(response_json))
            elif self.task_status == "FAILURE":
                success_fail = True
                print("Failure: AoI: {}, {}".format(self.geometry, response_json))
            else:
                retry += 1
                time.sleep(0.5 * retry)
        if retry == n_retries:
            self.task_status = "FAILED: Retry timeout"


if __name__ == "__main__":
    start_date = "01-01-2000"
    end_date = "12-31-2018"
    source = "nwis"
    dataset = "streamflow"
    module = "hydrology"
    t0 = time.time()
    hms = HMS(start_date=start_date,
              end_date=end_date,
              source=source,
              dataset=dataset,
              module=module)
    hms.set_geometry(metadata={"gaugestation": "02191300"})
    hms.submit_request()
    hms.print_info()
    t1 = time.time()
    print("Runtime: {} sec".format(round(t1-t0, 4)))

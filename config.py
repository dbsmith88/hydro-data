import os
import copy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

plt.style.use('seaborn-white')


class Configs:

    HMS_BASE_URL = os.getenv("HMS_URL", "https://ceamdev.ceeopdev.net/hms/rest/api/v3/")
    HMS_DATA_URL = os.getenv("HMS_DATA", "https://ceamdev.ceeopdev.net/hms/rest/api/v2/hms/data?job_id=")
    HMS_INFO_URL = os.getenv("HMS_INFO", "https://ceamdev.ceeopdev.net/hms/rest/api/info/catchment?comid=")
    HMS_COOKIE = {'sessionid': 'b5c5ev7usauevf2nro7e8mothmekqsnj'}

    DB_PATH = os.path.join(".", "catchment-data.sqlite3")
    GAGE_PATH = os.path.join(".", "data", "camels-gages.csv")
    SF_FILE_PATH = os.path.join(".", "data", "usgs_streamflow")

    # 25 years of data, 80/20 training/testing split => 20yr/5yr of data
    START_DATE = datetime(1980, 1, 1, 1)
    END_DATE = datetime(2010, 12, 31, 23)
    # START_DATE = datetime(2010, 1, 1, 1)
    # END_DATE = datetime(2015, 12, 31, 23)

    PRECIP_SOURCE = "nldas"
    TEMP_SOURCE = "nldas"
    RAD_SOURCE = "nldas"
    ET_SOURCE = "nldas"

    TIMESTEP = "hourly"
    INTERPOLATE = "linear"          # pandas replace missing values method options: linear(interpolate), backfill, ffill

    TARGET_TYPE = "section"
    TARGET_NAME = "EAST GULF COASTAL PLAIN"

    def __init__(self):
        pass

    def initialize_df(self):
        i_date = copy.copy(self.START_DATE)
        td = timedelta(hours=1) if self.TIMESTEP == "hourly" else timedelta(days=1)         # hourly or daily
        dates = []
        while i_date <= self.END_DATE:
            dates.append([i_date.year, i_date.month, i_date.day, i_date.hour])
            i_date = i_date + td
        date_columns = ["year", "month", "day", "hour"]
        df = pd.DataFrame(dates, columns=date_columns)
        return df

    def insert_timeseries(self, df: pd.DataFrame, columns: list, timeseries: dict, interpolate=None, plot=False, title=None, columns_i: list=None, minType=None):
        if not interpolate:
            interpolate = self.INTERPOLATE
        i_date = copy.copy(self.START_DATE)
        td = timedelta(hours=1) if self.TIMESTEP == "hourly" else timedelta(days=1)  # hourly or daily
        data = []
        c = len(columns_i) if columns_i else len(columns)
        missing_data = [np.nan for i in range(0, c)]
        while i_date <= self.END_DATE:
            values = []
            datestamp = i_date.strftime("%Y-%m-%d %H")
            if datestamp in timeseries.keys():
                if columns_i:
                    for i in columns_i:
                        if minType:
                            v = datetime.strptime(timeseries[datestamp][i], minType)
                        else:
                            v = float(timeseries[datestamp][i])
                        if int(v) == -9998 or int(v) == -9999:
                            values.append(np.nan)
                        else:
                            values.append(v)
                else:
                    for v in timeseries[datestamp]:
                        v = float(v)
                        if int(v) == -9998 or int(v) == -9999:
                            values.append(np.nan)
                        else:
                            values.append(v)
            else:
                values = missing_data
            data.append(values)
            i_date = i_date + td
        for i in range(0, len(data)):
            data[i] = np.asarray(data[i], dtype=np.float64)
        temp_data = data.copy()
        data_df = pd.DataFrame(data, columns=columns, dtype=np.float64)
        merge = True
        for c in columns:
            if interpolate in ["linear", "slinear", "quadratic", "cubic", "values"]:
                data_df[c] = data_df[c].interpolate(method=interpolate).ffill().bfill()
            elif interpolate in ["polynomial", "spline"]:
                data_df[c] = data_df[c].interpolate(method=interpolate, order=4).ffill().bfill()
            elif interpolate == "gaussian":
                merge = False
                df = df.join(data_df, how='outer')
                df = self.random_gaussian(df, columns)
            else:
                data_df[c] = data_df[c].fillna(method=interpolate).ffill().bfill()
        if merge:
            df = df.join(data_df, how='outer')
        if plot:
            plot_data = pd.DataFrame()
            plot_columns = columns
            for i in range(0, len(columns)):
                c = columns[i]
                c0 = c + "_0"
                d_i = df[c]
                plot_data[c] = temp_data[:, i]
                plot_data[c0] = d_i
                plot_columns.append(c0)
            x = pd.to_datetime(df[["year", "month", "day", "hour"]])
            plot_data["datetime"] = x
            plot_data.set_index('datetime')
            colors = ['b', 'm', 'g', 'c', 'y', 'k']
            ax = plot_data.plot(x='datetime', y=plot_columns[0], linewidth=1.0, label=plot_columns[0], color=colors[0], figsize=(16, 8))
            plot_data.plot(x='datetime', y=plot_columns[0], linewidth=1.0, label=plot_columns[0], color=colors[0],
                           figsize=(16, 8))
            for c in range(1, len(plot_columns)):
                plot_data.plot(x='datetime', y=plot_columns[c], linewidth=1.0, label=plot_columns[c], color=colors[0],
                               figsize=(16, 8))
                plot_data.plot(x='datetime', y=plot_columns[c], linewidth=0.5, label=plot_columns[c], color=colors[c], ax=ax)
            ax.set_title("{} - {} interpolation".format(title, interpolate))
            plt.show()
        return df

    def random_gaussian(self, df_data, columns):
        df = df_data.copy()
        df_mean = df.groupby(["month", "day"])[columns].mean()
        df_std = df.groupby(["month", "day"])[columns].std()
        df_mean_month = df.groupby(["month"])[columns].mean()
        mins = df[columns].min()
        seed = 42
        np.random.seed(seed)
        p = []
        p_day = None
        std_day = None
        mean_day = None
        mean_month = None
        data = {}
        for c in columns:
            data[c] = []
        for index, row in df.iterrows():
            if p_day is None or p_day != row['day']:
                r_std = df_std.loc[row['month']].loc[row['day']]
                r_mean = df_mean.loc[row['month']].loc[row['day']]
                m_mean = df_mean_month.loc[row['month']]
            else:
                r_std = std_day
                r_mean = mean_day
                m_mean = mean_month
            for i in range(0, len(columns)):
                temp_p = []
                c = columns[i]
                if np.isnan(r_std[c]):
                    r_std = std_day
                else:
                    std_day = r_std
                if np.isnan(r_mean[c]):
                    r_mean = mean_day
                else:
                    mean_day = r_mean
                if np.isnan(m_mean[c]):
                    m_mean = mean_day
                else:
                    mean_day = m_mean
                if np.isnan(row[c]):
                    c_std = r_std[c]
                    if len(p) > 0:
                        c_mean = (r_mean[c] + m_mean[c]) / 2.0
                    else:
                        c_mean = (r_mean[c] + m_mean[c]) / 2.0
                    v = -1
                    while v < mins[c]:
                        v = (np.random.normal(c_mean, c_std, 1)[0] + p[i]) / 2.0
                else:
                    v = row[c]
                temp_p.append(v)
                data[c].append(v)
                p = temp_p
        for c in columns:
            df[c] = data[c]
            df[c] = df[c].astype(np.float64)
        return df

    def load_from_file(self, gageid):
        columns = ["year", "month", "day", "hour", "q"]
        if len(gageid) < 8:
            gageid = "0{}".format(gageid)
        file_path = os.path.join(self.SF_FILE_PATH, "{}_streamflow_qc.txt".format(gageid))
        data = []
        with open(file_path, 'r') as sf_file:
            row_data = []
            for row in sf_file:
                raw_values = row.split(" ")
                values = []
                for v in raw_values:
                    if v != "":
                        values.append(v)
                if values[4] == -999.:
                    row_list = [int(values[1]), int(values[2]), int(values[3]), 0, np.nan]
                else:
                    row_list = [int(values[1]), int(values[2]), int(values[3]), 0, float(values[4])]
                row_data.append(row_list)
        df = pd.DataFrame(row_data, columns=columns)
        return df
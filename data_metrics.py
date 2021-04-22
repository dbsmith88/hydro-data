from db_controller import DBController
from main import load_gages
import pandas as pd
import numpy as np
import time
import datetime


START_DATE = datetime.datetime(1999, 10, 1)
END_DATE = datetime.datetime(2008, 9, 30)


def calculate_streamflow_metrics():
    db = DBController()
    gages = load_gages()

    columns = ["streamflow"]
    metrics_std = {}
    metrics_mean = {}
    first_metric = True

    for c in columns:
        metrics_mean[c] = None
        metrics_std[c] = None

    print("Calculating Mean and STD for {} gages".format(len(gages)))
    i = 0
    for g in gages:
        print("{}/{} Completed, GAGEID: {}".format(i, len(gages), g["SOURCE_FEA"]))

        query = "SELECT value FROM CatchmentData WHERE ComID=? AND parameter='AreaSqKM'"
        values = (g["FLComID"],)
        c = db.conn.cursor()
        c.execute(query, values)
        area = float(c.fetchone()[0])

        query = "SELECT * FROM StreamflowData WHERE gageID='{}'".format(g["SOURCE_FEA"])
        df = pd.read_sql_query(query, db.conn)
        dates = (df.year.map(str) + "/" + df.mnth.map(str) + "/" + df.day.map(str))
        df['date'] = pd.to_datetime(dates, format="%Y/%m/%d")

        date_mask = (df.date >= START_DATE) & (df.date <= END_DATE)
        df = df.loc[date_mask]
        dates = (df.year.map(str) + "/" + df.mnth.map(str) + "/" + df.day.map(str))
        df['date'] = pd.to_datetime(dates, format="%Y/%m/%d")

        for c in columns:
            df[c] = pd.to_numeric(df[c])

        agg_df = pd.DataFrame()
        # agg_df['streamflow'] = df[['streamflow']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).sum().streamflow
        agg_df['streamflow'] = (df['streamflow'] * 86400) / (area * 10**6)
        agg_df['streamflow'] = df['streamflow']
        # dates = (df.year.map(str) + "/" + df.mnth.map(str) + "/" + df.day.map(str)).unique()
        # agg_df.index = pd.to_datetime(dates, format="%Y/%m/%d")

        for c in columns:
            c_mean = agg_df[c].mean()
            c_std = agg_df[c].std()
            if first_metric:
                metrics_mean[c] = c_mean
                metrics_std[c] = c_std
            else:
                if not np.isnan(c_mean):
                    metrics_mean[c] = (metrics_mean[c] + c_mean) / 2.0
                if not np.isnan(c_std):
                    metrics_std[c] = (metrics_std[c] + c_std) / 2.0
        first_metric = False

        i += 1
    print("All gage metrics calculated.")
    print("STD: {}".format(metrics_std))
    print("MEAN: {}".format(metrics_mean))


def calculate_metrics():
    db = DBController()
    gages = load_gages()

    columns = ["prcp", "srad", "lrad", "swe", "tmax", "tmin", "vp", "et"]
    metrics_std = {}
    metrics_mean = {}
    first_metric = True

    for c in columns:
        metrics_mean[c] = None
        metrics_std[c] = None

    print("Calculating Mean and STD for {} gages".format(len(gages)))
    i = 0
    for g in gages:
        print("{}/{} Completed, GAGEID: {}".format(i, len(gages), g["SOURCE_FEA"]))
        query = "SELECT * FROM ForcingData WHERE gageID='{}'".format(g["SOURCE_FEA"])
        df = pd.read_sql_query(query, db.conn)
        dates = (df.year.map(str) + "/" + df.mnth.map(str) + "/" + df.day.map(str))
        df['date'] = pd.to_datetime(dates, format="%Y/%m/%d")

        date_mask = (df.date >= START_DATE) & (df.date <= END_DATE)
        df = df.loc[date_mask]
        dates = (df.year.map(str) + "/" + df.mnth.map(str) + "/" + df.day.map(str))
        df['date'] = pd.to_datetime(dates, format="%Y/%m/%d")

        for c in columns:
            df[c] = pd.to_numeric(df[c])
        df_temp = df.copy()

        agg_df = pd.DataFrame()
        agg_df['prcp'] = df[['prcp']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).sum().prcp
        agg_df['srad'] = df[['srad']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).mean().srad
        agg_df['lrad'] = df[['lrad']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).mean().lrad
        agg_df['swe'] = df[['swe']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).sum().swe
        agg_df['tmax'] = df[['tmax']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).max().tmax
        agg_df['tmin'] = df_temp[['tmax']].groupby([df_temp["date"].dt.year, df_temp["date"].dt.month, df_temp["date"].dt.day]).min().tmax
        agg_df['vp'] = df[['vp']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).mean().vp
        agg_df['et'] = df[['et']].groupby([df["date"].dt.year, df["date"].dt.month, df["date"].dt.day]).sum().et
        dates = (df.year.map(str) + "/" + df.mnth.map(str) + "/" + df.day.map(str)).unique()
        agg_df.index = pd.to_datetime(dates, format="%Y/%m/%d")

        for c in columns:
            c_mean = agg_df[c].mean()
            c_std = agg_df[c].std()
            if first_metric:
                metrics_mean[c] = c_mean
                metrics_std[c] = c_std
            else:
                if not np.isnan(c_mean):
                    metrics_mean[c] = (metrics_mean[c] + c_mean) / 2.0
                if not np.isnan(c_std):
                    metrics_std[c] = (metrics_std[c] + c_std) / 2.0
        first_metric = False

        i += 1
    print("All gage metrics calculated.")
    print("STD: {}".format(metrics_std))
    print("MEAN: {}".format(metrics_mean))


if __name__ == "__main__":
    t0 = time.time()
    calculate_streamflow_metrics()
    # calculate_metrics()
    t1 = time.time()
    print("Total Runtime: {} min".format(round(t1-t0, 4) / 60.))
#
# STD: {'prcp': 10.007500564603152, 'srad': 107.08220303864564, 'lrad': 30.000556446613594, 'swe': 201.25006759299765, 'tmax': 6.8093145665405785, 'tmin': 4.288644686248762, 'vp': 7281.311437861614, 'et': 0.571235454347913}
# MEAN: {'prcp': 3.712443580719469, 'srad': 208.72621428471857, 'lrad': 301.86262972212364, 'swe': 90.14117415174269, 'tmax': 290.9826865721821, 'tmin': 282.1923567598892, 'vp': 43193.262035473235, 'et': 1.0362304370796214}


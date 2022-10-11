import os
from glob import glob

import pandas as pd
import datetime as dt
import numpy as np

from tqdm import tqdm

from Data.Meta.config import MIN_STAY_THRESHOLD, MAX_STAY_RADIUS
from util import haversine


def stay_point_clustering(data:pd.DataFrame):
    stay_points = []
    n_stay_point = 0
    prev_stay_start = -1
    prev_stay_time = 0
    prev_stay = None
    for idx, (lat, long, dut) in enumerate(data[['latitude','longitude','duration']].values):
        if prev_stay is not None:
            dist = haversine((lat,long),prev_stay)
            if dist < MAX_STAY_RADIUS:
                prev_stay_time += dut
                continue
            else:
                if prev_stay_time > MIN_STAY_THRESHOLD:
                    stay_points += [n_stay_point]*(idx-prev_stay_start)
                    n_stay_point += 1
                else:
                    stay_points += [-1]*(idx-prev_stay_start)
                prev_stay = (lat, long)
                prev_stay_start = idx
                prev_stay_time = dut
        else:#only idx ==0
            assert idx == 0 
            prev_stay = (lat, long)
            prev_stay_start = idx
            prev_stay_time = 0
    if prev_stay is not None:
        if prev_stay_time > MIN_STAY_THRESHOLD:
            stay_points += [n_stay_point]*(idx-prev_stay_start+1)
            n_stay_point += 1
        else:
            stay_points += [-1]*(idx-prev_stay_start+1)
    data.loc[:, 'STAY'] = stay_points

    stays = data.query("STAY!=-1").groupby("STAY").agg(
        LAT = ("latitude","first"), 
        LONG = ("longitude","first"),
        DUT_MS = ('duration','sum'),
        END = ('timestamp','last'),
    )
    stays.loc[:,'START'] = stays['END'].values - stays['DUT_MS'].values

    stays.reset_index(inplace = True)
    return data, stays

if __name__ == "__main__":
    src = os.path.join("/mnt","Raws")
    valid_user = pd.read_csv(os.path.join("Data","Meta","valid_user.csv"), index_col= None, header = 0)
    valid_user.loc[:,'GPS'] = [np.nan] * valid_user.shape[0]
    valid_user.set_index('UID',inplace = True)
    
    for uid in tqdm(sorted(valid_user.query('VALID == "VALID"').index)):
        locations = []
        for file in glob(os.path.join(src, uid,"LocationEntity-*.csv")):
            df = pd.read_csv(file, index_col = None, header = 0)
            locations.append(df)
        assert len(locations) == 7
        location = pd.concat(locations, axis=0)
        location.sort_values(by = "timestamp", inplace = True)
        location.drop_duplicates(inplace = True)
        location.reset_index(drop = True, inplace =  True)
        location.loc[:,'duration'] = location['timestamp'].diff()

        dut = location.iloc[-1]['timestamp'] - location.iloc[0]['timestamp']

        location, stays = stay_point_clustering(location)
        location.to_csv(os.path.join("Log", uid+"_location.csv"), index = False)


        stays.loc[:,'UTC'] = ["UTC+0900"] * stays.shape[0] 
        stays.loc[:,"READABLE_DUT"] = pd.to_timedelta(stays["DUT_MS"], unit = 'ms')
        stays.loc[:,"READABLE_START"] = pd.to_datetime(stays["START"], unit = 'ms') + dt.timedelta(hours = 9)
        stays.loc[:,"READABLE_END"] = pd.to_datetime(stays["END"], unit = 'ms') + dt.timedelta(hours = 9)

        stays.sort_values(by = ["START"], inplace = True)
        stays[['STAY','START','END',
            'UTC','READABLE_START','READABLE_END',
            "DUT_MS", "READABLE_DUT",
            "LAT", "LONG"]].to_csv(os.path.join("Data","Interval",uid,"location_stay.csv"), index = False)
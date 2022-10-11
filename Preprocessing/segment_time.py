import os

import pandas as pd
import datetime as dt
import numpy as np

from tqdm import tqdm

from util import calc_intersection, load_interval_dataframe
from Data.Meta.config import *

src = os.path.join("Data","Interval")

def segment_time(uid: str, window_length_min: int):

    battery = load_interval_dataframe(os.path.join(src, uid, f"battery.csv"))
    app_usage = load_interval_dataframe(os.path.join(src, uid, f"app_usage_package.csv"))
    activity = load_interval_dataframe(os.path.join(src, uid, f"activity.csv"))
    location = load_interval_dataframe(os.path.join(src, uid, f"location.csv"))
    if (battery is None) or (app_usage is None) or (activity is None) or (location is None):
        return 
    
    #Trimming the timewindow
    START = max(battery.iloc[0]['START'],
                app_usage.iloc[0]['START'],
                activity.iloc[0]['START'],
                location.iloc[0]['START'])
    END = min(battery.iloc[-1]['END'],
            app_usage.iloc[-1]['END'],
            activity.iloc[-1]['END'],
            location.iloc[-1]['END'])
    # START segmented in 1HOUR unit
    START = (START // (60*60*1000) +1) * (60*60*1000)
    END = (END // (60*60*1000)) * (60*60*1000)

    #Get Significant App
    sig = app_usage.groupby('PACKAGE_ID').agg(total = ('START','count'))
    sig.reset_index(inplace = True)
    sig['isSig'] = [1 if val > sig['total'].mean() else 0 for val in sig['total'].values]
    sig_apps = sig.query('isSig == 1')['PACKAGE_ID'].values

    rows = []
    window_size = window_length_min*60*1000
    # print(uid)
    # print(START, END, window_size)
    # print(app_usage.shape, activity.shape, step_count.shape, location.shape)
    for window_start in np.arange(START, END, window_size):
        window_end = window_start + window_size
        # remove window that have missing for logging
        overlap = 0
        for start, end in battery.query("START < @window_end and END > @window_start")[['START','END']].values:
            overlap += calc_intersection(start, end, window_start, window_end)
            if overlap != window_size:
                break
        if overlap != window_size:
            continue
        row = {'START':window_start, 'END':window_end}
        for main in APP_CATEGORIES + APP_PREFERENCE:
            for prefix in APP_PREFIX:
                row[prefix + main] = 0
        for main in ACTIVITY_CATEGORIES:
            row['DUT_' + main] = 0
        for main in LOCATION_CATEGORIES:
            row['DUT_' + main] = 0
        
        for start, end, category, package in app_usage.query("START < @window_end and END > @window_start")[['START','END','CUSTOM_CATEGORY', 'PACKAGE_ID']].values:
            overlap = calc_intersection(start, end, window_start, window_end)
            if overlap == 0:
                continue
            row['UT_' + category] += overlap
            row['LC_' + category] += 1
            if package in sig_apps:
                row['UT_SIG'] += overlap
                row['LC_SIG'] += 1
            else:
                row['UT_NONSIG'] += overlap
                row['LC_NONSIG'] += 1
            row['UT_ALL'] += overlap
            row['LC_ALL'] += 1
        
        for start, end, category in activity.query("START < @window_end and END > @window_start")[['START','END','ACTIVITY']].values:
            overlap = calc_intersection(start, end, window_start, window_end)
            if overlap != 0:
                row['DUT_' + category] += overlap
        for start, end, category in location.query("START < @window_end and END > @window_start")[['START','END','CATEGORY']].values:
            overlap = calc_intersection(start, end, window_start, window_end)
            if overlap != 0:
                row['DUT_' + category] += overlap
        rows.append(row)
    timewindow = pd.DataFrame(rows)

    timewindow.loc[:,'UTC'] = ['UTC+0900'] * timewindow.shape[0]
    timewindow.loc[:,'READABLE_START'] = pd.to_datetime(timewindow['START'], unit = 'ms') + dt.timedelta(hours = 9)
    timewindow.loc[:,'READABLE_END'] = pd.to_datetime(timewindow['END'], unit = 'ms') + dt.timedelta(hours = 9)
    timewindow.loc[:,'DUT_SEDENTARY'] = timewindow['DUT_STILL'].values + timewindow['DUT_IN_VEHICLE'].values

    return timewindow[['START','END','UTC','READABLE_START','READABLE_END',\
        *OVERALL_APP_USAGE, *SIGNIFICANT_APP_USAGE, *CATEGORICAL_APP_USAGE,\
        *ACTIVITY_CONTEXT, *LOCATION_CONTEXT, *PA]]

if __name__ == "__main__":
    target = os.path.join("Data", "15Min")
    if not os.path.exists(target):
        os.makedirs(target)
    
    valid_user = pd.read_csv(os.path.join("Data","Meta","valid_user.csv"), index_col= None, header = 0)
    valid_user.set_index("UID",inplace = True)
    for uid in tqdm(sorted(valid_user.query("VALID == 'VALID'").index)):
        timewindow = segment_time(uid, 15)
        if timewindow is not None:
            timewindow.to_csv(os.path.join(target, uid +'.csv'), index = False)
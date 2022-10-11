import os
from glob import glob

import pandas as pd
import datetime as dt
import numpy as np

from tqdm import tqdm

from util import matchStartEnd

def matchScreenEvent(df: pd.DataFrame)-> pd.DataFrame:
    events = {'MOVE_TO_FOREGROUND':'start', 'MOVE_TO_BACKGROUND':'end'} 
    event_types = list(events.keys())
    # remove unneccessary columns and rows
    df = df[['TIMESTAMP','PACKAGE_ID', 'EVENT_TYPE']]
    df = df.drop_duplicates()
    df = df.query('EVENT_TYPE in @event_types')

    # make it EVENT_TYPE to start and end
    df = df.assign(EVENT = [events[val] for val in df['EVENT_TYPE'].values])
    df.rename({'PACKAGE_ID':'ID'}, axis = 1, inplace= True)
    df = matchStartEnd(df)
    df.rename({'ID':'PACKAGE_ID'}, axis = 1, inplace= True)
    return df

if __name__ == '__main__':
    src = os.path.join("/mnt","Raws")

    valid_user = pd.read_csv(os.path.join("Data","Meta","valid_user.csv"), index_col= None, header = 0)
    app_label = pd.read_csv(os.path.join("Data","Meta", "app_package_info.csv"), index_col = None, header = 0)
    
    valid_user.loc[:,'APP_USAGE'] = [np.nan] * valid_user.shape[0]

    for_list = valid_user[['UID', 'VALID']].values
    valid_user.set_index("UID",inplace = True)
    for uid, valid in tqdm(for_list):
        if valid =="INVALID":
            continue
        app_usages = []
        for file in glob(os.path.join(src, uid, "AppUsageEventEntity-*.csv")):
            df = pd.read_csv(file)
            if df.shape[0] == 0:
                break
            app_usages.append(df)
        if len(app_usages) != 7:
            valid_user.loc[uid, 'APP_USAGE'] = f'{7-len(app_usages)} day missing'
            continue
        app_usage = pd.concat(app_usages, axis = 0)

        app_usage.rename({'timestamp':'TIMESTAMP', 'packageName':'PACKAGE_ID', 'type':'EVENT_TYPE'}, axis = 1, inplace= True)
        app_usage = matchScreenEvent(app_usage)

        app_usage.loc[:,'UTC'] = ['UTC+0900'] * app_usage.shape[0]
        app_usage.loc[:,'READABLE_TIMESTAMP'] = pd.to_datetime(app_usage['TIMESTAMP'], unit='ms') + dt.timedelta(hours = 9)

        #Save Intermediate Result for logging
        app_usage[['TIMESTAMP','UTC','READABLE_TIMESTAMP', 'PACKAGE_ID', 'EVENT','STATE', 'MATCH']].to_csv(os.path.join("Log",f"app_usage_{uid}.csv"), index = False)
        
        valid_match = ['start','end']
        match_ratio  =app_usage.query("MATCH in @valid_match").shape[0]/app_usage.shape[0]
        if match_ratio < .99:
            valid_user.loc[uid, 'APP_USAGE'] = '{:.1f}% was not matched'.format((1-match_ratio)*100)
            valid_user.loc[uid, 'VALID'] = 'INVALID'
            continue
    
        #Remove Logging App Usage
        logging_apps = ['com.pacoapp.paco', 'kaist.iclab.abclogger', 'iclab.kaist.ac.kr.msband_logger', 'fi.polar.beat', 'com.microsoft.kapp']
        app_usage.query('PACKAGE_ID not in @logging_apps', inplace = True)

        #Make Matched event into Interval
        interval = app_usage.query("MATCH in @valid_match")
        interval.reset_index(drop = True, inplace= True)

        interval_start, interval_end = interval.iloc[::2], interval.iloc[1::2]
        interval_start.reset_index(inplace = True, drop= True)
        interval_end.reset_index(inplace = True, drop = True)

        interval_start.columns = ['START_'+val for val in interval_start.columns]
        interval_end.columns = ['END_'+val for val in interval_end.columns]
        interval = pd.concat([interval_start, interval_end], axis = 1)
        interval.rename({'START_TIMESTAMP':'START', 'END_TIMESTAMP':'END', 'START_PACKAGE_ID': 'PACKAGE_ID'}, axis = 1, inplace = True)
        interval = interval[['START', 'END', 'PACKAGE_ID']]

        # Add CUSTOM_CATEGORY of each PACKAGE_ID
        interval.loc[:, 'CUSTOM_CATEGORY'] = [app_label.query('PACKAGE == @val').iloc[0]['CUSTOM_CATEGORY'] for val in interval['PACKAGE_ID'].values]

        #Add Human Readable Time
        interval.loc[:,'UTC'] = ['UTC+0900'] * interval.shape[0]
        interval.loc[:,'READABLE_START'] = pd.to_datetime(interval['START'], unit='ms') + dt.timedelta(hours = 9)
        interval.loc[:,'READABLE_END'] = pd.to_datetime(interval['END'], unit='ms') + dt.timedelta(hours = 9)

        #sorting columns
        interval = interval[['START','END','UTC', 'READABLE_START', 'READABLE_END', 'PACKAGE_ID', 'CUSTOM_CATEGORY']]
        interval.query("CUSTOM_CATEGORY != 'UNKNOWN'", inplace = True)

        interval.to_csv(os.path.join("Data","Interval", uid, f"app_usage_package.csv"), index = False)
        interval[['START','END','UTC', 'READABLE_START', 'READABLE_END','CUSTOM_CATEGORY']].to_csv(os.path.join("Data","Interval", uid, f"app_usage.csv"), index = False)
    valid_user.reset_index(inplace = True)
    valid_user.to_csv(os.path.join("Data","Meta","valid_user.csv"),index = False)
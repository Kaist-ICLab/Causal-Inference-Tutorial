
import os
from glob import glob

import pandas as pd
import datetime as dt
import numpy as np

from tqdm import tqdm

from util import matchStartEnd

def matchActivityEvent(df: pd.DataFrame)-> pd.DataFrame:
    # remove unneccessary columns and rows
    df = df[['TIMESTAMP','ACTIVITY', 'EVENT']]
    df = df.drop_duplicates()

    df.rename({'ACTIVITY':'ID'}, axis = 1, inplace= True)
    df = matchStartEnd(df)
    df.rename({'ID':'ACTIVITY'}, axis = 1, inplace= True)
    return df

if __name__ == '__main__':
    valid_user = pd.read_csv(os.path.join("Data","Meta","valid_user.csv"), index_col= None, header = 0)
    valid_user.set_index("UID",inplace = True)
    valid_user.loc[:,'ACTIVITY'] = [np.nan]* valid_user.shape[0]
    for uid in tqdm(sorted(valid_user.query('VALID == "VALID"').index)):
        activities = []
        for file in glob(os.path.join("/mnt","Raws", uid, "PhysicalActivityTransitionEntity-*.csv")):
            activities.append(pd.read_csv(file, index_col = None, header = 0))
        activity = pd.concat(activities, axis = 0)

        event_type = {'ENTER':'start', 'EXIT':'end'}
        activity = activity.assign(
            EVENT = [ event_type[val.split("_",1)[0]] for val in activity['transitionType'].values],
            ACTIVITY = [val.split("_",1)[1] for val in activity['transitionType'].values])

        activity.rename({'timestamp':'TIMESTAMP'}, axis = 1, inplace= True)
        activity = matchActivityEvent(activity)

        activity.loc[:,'UTC'] = ['UTC+0900'] * activity.shape[0]
        activity.loc[:,'READABLE_TIMESTAMP'] = pd.to_datetime(activity['TIMESTAMP'], unit='ms') + dt.timedelta(hours = 9)

        #Save Intermediate Result for logging
        activity[['TIMESTAMP','UTC','READABLE_TIMESTAMP','ACTIVITY', 'EVENT','MATCH']]\
            .to_csv(os.path.join("Log",f"activity_{uid}.csv"), index = False)
        
        #Make Matched event into Interval
        valid_match = ["start", "end"]
        match_ratio  =activity.query("MATCH in @valid_match").shape[0]/activity.shape[0]
        if match_ratio < .99:
            valid_user.loc[uid, 'ACTIVITY'] = '{:.1f}% was not matched'.format((1-match_ratio)*100)
            valid_user.loc[uid, 'VALID'] = 'INVALID'
            continue
    
        interval = activity.query("MATCH in @valid_match")
        interval.reset_index(drop = True, inplace= True)

        interval_start, interval_end = interval.iloc[::2], interval.iloc[1::2]
        interval_start.reset_index(inplace = True, drop= True)
        interval_end.reset_index(inplace = True, drop = True)
        
        interval_start.columns = ['START_'+val for val in interval_start.columns]
        interval_end.columns = ['END_'+val for val in interval_end.columns]
        interval = pd.concat([interval_start, interval_end], axis = 1)
        interval.rename({'START_TIMESTAMP':'START', 'END_TIMESTAMP':'END', 'START_ACTIVITY': 'ACTIVITY'}, axis = 1, inplace = True)
        interval = interval[['START', 'END', 'ACTIVITY']]

        #Add Human Readable Time
        interval.loc[:,'UTC'] = ['UTC+0900'] * interval.shape[0]
        interval.loc[:,'READABLE_START'] = pd.to_datetime(interval['START'], unit='ms') + dt.timedelta(hours = 9)
        interval.loc[:,'READABLE_END'] = pd.to_datetime(interval['END'], unit='ms') + dt.timedelta(hours = 9)
        
        interval[['START', 'END', 'UTC', 'READABLE_START','READABLE_END', 'ACTIVITY']].to_csv(os.path.join("Data","Interval", uid, f"activity.csv"), index = False)
    valid_user.reset_index(inplace = True)
    valid_user.to_csv(os.path.join("Data","Meta","valid_user.csv"), index = False)
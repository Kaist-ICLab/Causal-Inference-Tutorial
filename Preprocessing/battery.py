import os
from glob import glob

import pandas as pd
import datetime as dt
import numpy as np

from tqdm import tqdm

from Data.Meta.config import MAX_SAMPLING_PERIOD

src = os.path.join("/mnt", "Raws")

valids = []
for uid in tqdm(sorted(os.listdir(src))):
    batterys = []
    for file in glob(os.path.join(src,uid, "BatteryEntity-*.csv")):
        df = pd.read_csv(file, index_col = False, header = 0)
        if df.shape[0] == 0: #concat dfs including empty dataframe make df not to recongnize type of each column
            continue
        batterys.append(df)
    if len(batterys) < 7:
        valids.append({'UID':uid, 'VALID': 'INVALID', 'BATTERY': "{} file of 7 days are missing".format(7-len(batterys))})
        continue
    battery = pd.concat(batterys, axis = 0)
    
    battery.drop_duplicates(inplace= True)
    battery.sort_values(by = "timestamp", inplace = True)
    battery.loc[:, "READABLE_TIMESTAMP"] = pd.to_datetime(battery['timestamp'], unit = 'ms') + dt.timedelta(hours = 9)
    battery.loc[:,'UTC'] = ['UTC+0900'] * battery.shape[0]
    battery.loc[:,'DIFF'] = battery['timestamp'].diff()
    battery['DIFF'].fillna(0, inplace= True)
    
    # [0, 30*60*1000) is acceptable timestamp DIFF
    # [0%, 100.0%] range for P0701
    battery.loc[:, 'VALID_SAMPLING_RATE'] = ['' if val < MAX_SAMPLING_PERIOD else 'INVALID' for val in battery['DIFF'].values]
    battery.rename({'timestamp':'TIMESTAMP'}, axis = 1, inplace = True)
    battery[['TIMESTAMP','UTC','READABLE_TIMESTAMP','DIFF', 'VALID_SAMPLING_RATE']].to_csv(os.path.join("Log", f"battery_{uid}.csv"), index = False)

    # logging time as interval
    # make group_index to grouping using groupby method
    invalid = [1 if val == "INVALID" else 0 for val in battery['VALID_SAMPLING_RATE'].values]
    valid_start = [0, *[1 if val == "INVALID" else 0 for val in battery['VALID_SAMPLING_RATE'].values[:-1]]]
    gidx = np.cumsum(np.array(invalid) + np.array(valid_start))
    battery.loc[:, 'GROUP_IDX'] = gidx

    logging = battery.groupby('GROUP_IDX').agg(CNT = ('TIMESTAMP','count'), START = ('TIMESTAMP', 'first'), END = ('TIMESTAMP', 'last'))
    logging.query("CNT > 1",inplace = True) #CNT == 1 means INVALID

    logging.loc[:,'UTC'] = ['UTC+0900'] * logging.shape[0]
    logging.loc[:,'READABLE_START'] = pd.to_datetime(logging['START'], unit = 'ms') + dt.timedelta(hours = 9)
    logging.loc[:,'READABLE_END'] = pd.to_datetime(logging['END'], unit = 'ms') + dt.timedelta(hours = 9)

    if not os.path.exists(os.path.join("Data","Interval", uid)):
        os.makedirs(os.path.join("Data","Interval", uid))
    logging[['START','END', 'UTC', 'READABLE_START', 'READABLE_END']].to_csv(os.path.join("Data","Interval", uid,"battery.csv"), index = False)
    logging.loc[:,'DURATION_MS'] = logging['END'].values - logging['START'].values
    total = logging['DURATION_MS'].sum() / (24*60*60*1000) 
    if total >= 5:
        valids.append({'UID':uid, 'VALID': 'VALID', 'BATTERY': 'Logged for {:.1f} days'.format(total)})
    else:
        valids.append({'UID':uid, 'VALID': 'INVALID', 'BATTERY': 'Logged for {:.1f} days'.format(total)})
valid = pd.DataFrame(valids)
valid.to_csv(os.path.join("Data","Meta", "valid_user.csv"), index = False)

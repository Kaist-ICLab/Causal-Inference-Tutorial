import os
import numpy as np
import pandas as pd


from math import radians, cos, sin, asin, sqrt
from typing import Iterable

def haversine(x:Iterable[float], y:Iterable[float]):
    """
    Calculate the great circle distance in kilometers between two points  
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lat1, lon1 = x
    lat2, lon2 = y
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r * 1000

def calc_intersection(st, ed, istart, iend):
    if st< istart:
        if ed < istart:
            return 0
        elif ed < iend:
            return ed-istart
        else:
            return iend-istart
    elif st< iend:
        if ed < iend:
            return ed-st
        else:
            return iend-st
    else:
        return 0

def load_interval_dataframe(path):
    if not os.path.exists(path):
        return
    df = pd.read_csv(path, index_col = None, header = 0)
    if df.shape[0] == 0:
        return
    for readable_timestamp in ["READABLE_START", "READABLE_END"]:
        df.loc[:,readable_timestamp] = pd.to_datetime(df[readable_timestamp])
    return df

def matchStartEnd(df:pd.DataFrame, ) -> pd.DataFrame:
    '''
    Parameter

    df: dataframe with columns; ID, EVENT, TIMESTAMP
        EVENT is one of start or end
        ID is identifier of event to be matched
    
    Return

    new_df: dataframe which info of match is included as column on original dataframe

    ---------------------------------------------------------------------------------
    Implementation

    Events of start and end that are consecutive and having same id will be matched to make interval.
    However, for some reasons, start and end do not have matched start/end or repeated. We 
    just pick most longest consecutive interval as:
        A_start, A_start, A_end => remove second A_start
        A_start, A_end, A_end => remove first A_end
    '''
    #remove unneccssary rows
    df = df[['TIMESTAMP','ID', 'EVENT']]
    events = ['end','start']
    df = df.assign(STATE = [events.index(val) for val in df['EVENT'].values])
    df.sort_values(by = ["TIMESTAMP","STATE"], inplace = True)
    df.drop_duplicates()
    df.reset_index(inplace = True, drop= True)

    match_info = []
    #prev_id means id of prev event
    #start mean start_timestamp, end mean end_timestamp of prev interval
    prev_id, start, end = None, None, None
    start_idx, end_idx = None, None
    for idx, (timestamp, id, state) in enumerate(df[['TIMESTAMP','ID','STATE']].values):
        if state == events.index('start'):
            if end is not None:#new interval is started
                match_info.append('start')
                
                prev_id = id
                start = timestamp
                start_idx = idx
                end = None
            elif start is not None:# start was left
                if id != prev_id:
                    match_info.append('start')
                    match_info[start_idx] = 'No Matched End'
                    
                    prev_id = id
                    start = timestamp
                    start_idx = idx
                else: 
                    match_info.append('Repeated Start')
            else: #very new start
                match_info.append('start')
                prev_id = id
                start = timestamp
                start_idx = idx
        else: 
            if end is not None: #interval ended
                if prev_id == id:
                    match_info.append('end')
                    match_info[end_idx] = 'End Repeated'
                    
                    end = timestamp
                    end_idx = idx
                else:
                    match_info.append('No Matched Start')

                    prev_id = None
                    start = None
                    end = None
            elif start is not None:
                if prev_id == id:
                    match_info.append('end')

                    end = timestamp
                    end_idx = idx
                else:
                    match_info.append('No Matched Start')
                    match_info[start_idx] = 'No Matched End'

                    prev_id = None
                    start = None
            else:
                match_info.append('No Matched Start')
    #Processing last row here
    if end is not None:
        pass
    elif start is not None:
        match_info[start_idx] = 'No Matched End'
    else:
        pass
    df = df.assign(MATCH = match_info)
    return df

def chunkConsecutive(df: pd.DataFrame, state: str):
    '''
    Parameter

    df: dataframe with columns; state, start, end
    
    Return

    new_df: aggregated dataframe

    ---------------------------------------------------------------------------------
    Implementation

    Aggregate consecutive identical state
    '''
    state_type = list(df[state].unique())
    df.sort_values(by= "START", inplace = True)
    df['state_'] = [state_type.index(val) for val in df[state].values]
    state_diff = [0, *df['state_'].diff()[1:]]
    state_diff = [0 if val == 0 else 1 for val in state_diff]
    group_idx = np.cumsum(state_diff)
    df['group_idx'] = group_idx
    df = df.groupby('group_idx').agg(**{'START': ('START','min'), 
                                    'END':('END','max'),
                                    state: (state, 'first')})
    df.reset_index(drop = True, inplace = True)
    return df
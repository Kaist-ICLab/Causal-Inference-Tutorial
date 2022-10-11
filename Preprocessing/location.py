import os

from tqdm import tqdm
import pandas as pd
import numpy as np


from util import haversine

if __name__ =="__main__":
    home_work_info = pd.read_csv(os.path.join("Data","Meta", "poi_category.csv"), index_col= False, header = 0)
    
    valid_user = pd.read_csv(os.path.join("Data","Meta","valid_user.csv"), index_col= None, header = 0)
    valid_user.set_index("UID",inplace = True)
    valid_user.loc[:,'LOCATION'] = [np.nan]* valid_user.shape[0]
    for uid in tqdm(sorted(valid_user.query('VALID == "VALID"').index)):
        file = os.path.join("Data", "Interval", uid, "location_stay.csv")
        if not os.path.exists(file):
            continue
        location = pd.read_csv(file, index_col = False, header = 0)
        location.loc[:,'CATEGORY'] = ['OTHERS'] * location.shape[0]
        location.reset_index(inplace = True)
        category_info = home_work_info.query("UID == @uid")
        for idx, lat,long in location[['index','LAT','LONG']].values:
            for jdx, (poi_lat, poi_long, category) in enumerate(category_info[['LAT','LONG','CATEGORY']].values):
                if haversine((lat, long),(poi_lat,poi_long)) < 75:
                    location.loc[idx, 'CATEGORY'] = category
                    break
        if len(location["CATEGORY"].unique()) == 1:
            valid_user.loc[uid,'LOCATION'] =  f"Only {location['CATEGORY'].unique()[0]} was found"
            valid_user.loc[uid,'VALID'] = 'INVALID'

        location[['START','END','UTC','READABLE_START','READABLE_END','CATEGORY']]\
            .to_csv(os.path.join("Data","Interval",uid,"location.csv"),index = False)
        location[['START','END','UTC','READABLE_START','READABLE_END','DUT_MS','READABLE_DUT','LAT','LONG','CATEGORY']]\
            .to_csv(os.path.join("Data","Interval",uid,"location_stay.csv"),index = False)
    valid_user.reset_index(inplace = True)
    valid_user.to_csv(os.path.join("Data","Meta","valid_user.csv"), index = False)
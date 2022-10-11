from typing import List

from itertools import product

# 
MAX_SAMPLING_PERIOD = 30*60 * 1000

# Hyper Parameter used in our study
MIN_STAY_THRESHOLD = 15 * 60 * 1000 # As milliseconds, no location change more than 15 minutes will be considered as stay point
MAX_STAY_RADIUS = 25 # As Meter, distance less than 25 m will be considered as same point

# While identifying logging time
MAX_LOSS_THRESHOLD = 15 * 60 *1000 # No Logging for more than 15 minutes for Battery Entity will be considered as loggin was disrupted.


# Extracting Feature based on timewindow length of
WINDOW_LEN_MS = 15 * 60 * 1000


APP_PREFIX = ['UT_','LC_']
def list_app_usage_features(types: List[str]) -> List[str]:
    return [prefix + main for prefix, main in  product(APP_PREFIX,types)]

PA = ['DUT_SEDENTARY']

LOCATION_CATEGORIES = ['HOME','WORK','OTHERS', 'MOVE']
TIME_SLOT = 4
ACTIVITY_CATEGORIES = ['STILL','WALKING','RUNNING','IN_VEHICLE','ON_BICYCLE']
APP_CATEGORIES = ['WORK','SOCIAL','INFO','ENTER','SYSTEM', 'HEALTH']
APP_PREFERENCE = ['ALL', 'SIG', 'NONSIG']

ACTIVITY_CONTEXT = ['DUT_' + val for val in ACTIVITY_CATEGORIES]
LOCATION_CONTEXT = ['DUT_' + loc for loc in LOCATION_CATEGORIES]
TIME_CONTEXT =  ['TIME_' + str(i) for i in range(TIME_SLOT)]

OVERALL_APP_USAGE = list_app_usage_features(['ALL'])
SIGNIFICANT_APP_USAGE = list_app_usage_features(['SIG', 'NONSIG'])
CATEGORICAL_APP_USAGE = list_app_usage_features(APP_CATEGORIES)

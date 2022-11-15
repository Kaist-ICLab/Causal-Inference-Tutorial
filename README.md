# A Tutorial on Causal Analysis of Human Behavior Using Mobile Sensor Data

This repository includes:

* Preprocessing Code of GPS, App Usage, and Activity
    * GPS was categorized into HOME, WORK, OTHERS, and MOVE
    * Application was categorized into SOCIAL, ENTER, HEALTH, WORK, SYSTEM
    * Activity predicted by Android API: STILL, WALKING, RUNNING, IN_VEHICLE, ON_BICYCLE

* Preprocessed Dataset used for the causal inference.
    * 3 types of data were all preprocessed into 15 Minute Interval.
        * Launch Count, Usage Time of each app category 
        * Duration of still(DUT_SEDENTARY) and Location(DUT_HOME, DUT_WORK, DUT_OTHER)
    
* Causal Inference Code for Preprocessed Dataset 
    * The code is written in R.

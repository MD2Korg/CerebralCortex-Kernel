import pandas as pd
import numpy as np
from collections import Counter


def preprocess_activity_data(activity_data,home):
    """
    Takes 10 mins worth of data from google activity api as a pnadas dataframe and produces
    feature vector from the dataframe.An example of raw data from google activity api used
    for model building purpose is provided as example.csv.
    time offset is ignored in this case assuming it same for every participant
    :param activity_data: pandas data frame from google activity api for 10 mins
    :param home: 1 if home 0 if not home
    :return: returns a numpy array as a feature vector for making predictions
    """
    # loading up the activity data and setting time as index of dataframe
    data = activity_data.set_index(0)
    # timestamp for the input google activity is unix timestamp so converting it into datetime
    data.index = pd.to_datetime(data.index, unit='ms')
    # sampling 10 mins worth of data into equal samples at 100ms each
    data_resampled = data.resample('100ms').pad()
    # for each timesample not present, assiging activity of previous timesample that is present
    data_resampled = data_resampled.fillna(method='bfill')
    # counting number of times each activity occured within 10 mins window
    activity_counts = Counter(data_resampled[2])
    feature = []
    activity_vector = [0]*8
    for values in activity_counts:
        # normalizing each activity counts based on 10 mins (6000 samples present in 10 mins)
        activity_vector[int(values)]= activity_counts[values]/6000
    # for time feature, taking time of last sample of 10 mins window as ema-time
    time = data_resampled.index[-1]
    hour_ = time.hour
    #day_of_week ={0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
    weekday = time.weekday()
    # working hour as feature, taking 9 to 5 as working hour
    working_hour = 1 if (hour_ >=9 and hour_ <=17) else 0
    # taking weekend as feature
    weekend = 1 if (weekday ==5 or weekday == 6) else 0
    feature.extend(activity_vector)
    feature.extend([home,working_hour,weekend])
    feature_vector = np.array(feature) 
    return feature_vector




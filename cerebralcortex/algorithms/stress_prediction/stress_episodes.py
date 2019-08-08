# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from typing import List
import numpy as np
from scipy import signal
from scipy.stats import iqr
from scipy.stats.mstats_basic import winsorize
from enum import Enum
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, ArrayType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import datetime

NOTSTRESS = "NOTSTRESS"
UNSURE = 'UNSURE'
YESSTRESS = 'YESSTRESS'
UNKNOWN = 'UNKNOWN'
NOTCLASSIFIED = 'NOTCLASSIFIED'

schema = StructType([
    StructField("user", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("stress_episode", StringType()),
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def stress_episodes_estimation(stress_data: object) -> object:
    # --- Constants definitions --- 
    smoothing_window = 3 # FIXME - 3 minutes
    macd_param_fast = 7;
    macd_param_slow = 19;
    macd_param_signal = 2;
    threshold_yes = 0.36;
    threshold_no = 0.36;

    data = impute(stress_data)

    # Smooth the stress values
    stress_smoothed_list = []

    for c in range(2,len(data['stress_probability'].values)):
        smoothed_stress = (data['stress_probability'].values[c] + \
                         data['stress_probability'].values[c-1] + \
                         data['stress_probability'].values[c-2]) / smoothing_window
        stress_smoothed_list.append((data['timestamp'].values[c], smoothed_stress))

    ema_fast_list = []
    ema_fast_list.append(stress_smoothed_list[0])
    ema_slow_list = []
    ema_slow_list.append(stress_smoothed_list[0])
    ema_signal_list = []
    ema_signal_list.append((0,0))
    histogram_list = []

    stress_episode_start = []
    stress_episode_peak = []
    stress_episode_classification = []
    stress_episode_intervals = []

    for c in range(len(stress_smoothed_list)):
        ema_fast_prev = ema_fast_list[-1][1]
        ema_fast_current = stress_smoothed_list[c][1]
        ema_fast = ewma(ema_fast_current, ema_fast_prev, 2.0/(macd_param_fast + 1))
        ema_fast_list.append((stress_smoothed_list[c][0], ema_fast))

        ema_slow_prev = ema_slow_list[-1][1]
        ema_slow_current = stress_smoothed_list[c][1]
        ema_slow = ewma(ema_slow_current, ema_slow_prev, 2.0/(macd_param_slow + 1))
        ema_slow_list.append((stress_smoothed_list[c][0], ema_slow))

        macd_prev = ema_fast_prev - ema_slow_prev
        macd_current = ema_fast_current - ema_slow_current
        ema_signal_prev = ema_signal_list[-1][1]
        ema_signal = ewma(macd_current, macd_prev, 2.0/(macd_param_signal + 1))
        ema_signal_list.append((stress_smoothed_list[c][0], ema_signal))

        histogram_prev =  macd_prev - ema_signal_prev
        histogram = macd_current - ema_signal
        histogram_list.append((stress_smoothed_list[c][0], histogram))

        if histogram_prev <=0 and histogram > 0:
            # Episode has ended, started increasing again
            start_timestamp = -1;
            peak_timestamp = -1;
            end_timestamp = stress_smoothed_list[c][0]
            stress_class = -1
            if len(stress_episode_start):
                start_timestamp = stress_episode_start[-1][0]

            if len(stress_episode_classification):
                peak_timestamp = stress_episode_classification[-1][0]
                stress_class = stress_episode_classification[-1][1]

            if stress_class != -1:
                #print('Found full stress episode', stress_class)
                #TODO - Handle this?????
                stress_episode_timestamps = []
                stress_episode_timestamps.append(start_timestamp) 
                stress_episode_timestamps.append(peak_timestamp) 
                stress_episode_timestamps.append(end_timestamp) 
                stress_episode_timestamps.append(stress_class) 
                stress_episode_intervals.append(stress_episode_timestamps) 

        if histogram_prev >=0 and histogram < 0:
            # Episode is in the middle, started decreasing
            episode_start_timestamp = get_episode_start_timestamp(stress_episode_classification, histogram_list, stress_smoothed_list[c][0]) 
            if episode_start_timestamp == -1:
                stress_episode_start.append((episode_start_timestamp, NOTCLASSIFIED))
                stress_episode_peak.append((stress_smoothed_list[c][0], NOTCLASSIFIED))
                stress_episode_classification.append((stress_smoothed_list[c][0], NOTCLASSIFIED))
            else:
                proportion_available = get_proportion_available(data, episode_start_timestamp, stress_smoothed_list[c][0]) 
                if proportion_available < 0.5:
                    stress_episode_start.append((episode_start_timestamp, UNKNOWN))
                    stress_episode_peak.append((stress_smoothed_list[c][0], UNKNOWN))
                    stress_episode_classification.append((stress_smoothed_list[c][0], UNKNOWN))
                else:
                    historical_stress = get_historical_values_timestamp_based(stress_smoothed_list, episode_start_timestamp, stress_smoothed_list[c][0])
                    if not len(historical_stress):
                        stress_episode_start.append((episode_start_timestamp, UNKNOWN))
                        stress_episode_peak.append((stress_smoothed_list[c][0], UNKNOWN))
                        stress_episode_classification.append((stress_smoothed_list[c][0], UNKNOWN))
                    else:
                        cumu_sum = 0.0
                        for hs in historical_stress:
                            cumu_sum += hs[1]
                        stress_density = cumu_sum / len(historical_stress)
                        if stress_density >= threshold_yes:
                            stress_episode_start.append((episode_start_timestamp, YESSTRESS))
                            stress_episode_peak.append((stress_smoothed_list[c][0], YESSTRESS))
                            stress_episode_classification.append((stress_smoothed_list[c][0], YESSTRESS))
                        elif stress_density <= threshold_no:
                            stress_episode_start.append((episode_start_timestamp, NOTSTRESS))
                            stress_episode_peak.append((stress_smoothed_list[c][0], NOTSTRESS))
                            stress_episode_classification.append((stress_smoothed_list[c][0], NOTSTRESS))
                        else:
                            stress_episode_start.append((episode_start_timestamp, UNSURE))
                            stress_episode_peak.append((stress_smoothed_list[c][0], UNSURE))
                            stress_episode_classification.append((stress_smoothed_list[c][0], UNSURE))
    

    stress_episode_df = pd.DataFrame(index = np.arange(0, len(stress_episode_classification)), columns=['user', 'timestamp', 'stress_episode'])
    user = data['user'].values[0]
    index = 0
    for c in stress_episode_classification:
        ts = c[0]
        status = c[1]
        stress_episode_df.loc[index] = [user, ts, status]
        index += 1

    return stress_episode_df

def ewma(x, y, alpha):
    return alpha * x + (1 - alpha) * y

def get_episode_start_timestamp(stress_episode_classification, histogram_list, currenttime):
    timestamp_prev = -1
    if len(stress_episode_classification) >= 3:
        timestamp_prev = stress_episode_classification[-3][0]
    elif len(stress_episode_classification) == 2:
        timestamp_prev = stress_episode_classification[-2][0]
    elif len(stress_episode_classification) == 1:
        timestamp_prev = stress_episode_classification[-1][0]

    histogram_history =  get_historical_values_timestamp_based(histogram_list, timestamp_prev, currenttime)

    if len(histogram_history) <= 1:
        return -1

    for x in range(len(histogram_history)-2 , -1, -1):
        if histogram_history[x][1] <= 0:
            return histogram_history[x+1][0]

    return histogram_history[0][0]


def get_historical_values_timestamp_based(data, start_timestamp, currenttime):
    toreturn = []
    starttime = start_timestamp
    if starttime == -1:
        starttime = currenttime - np.timedelta64(100*365*24*3600, 's') # approx 100 year to approximate -1
    for c in data:
        if c[0] >= starttime and c[0] <= currenttime:
            toreturn.append(c)
        if c[0] > currenttime:
            break

    return toreturn


def get_proportion_available(data, st, current_timestamp):
    count = 0
    available = 0
    start_timestamp = st
    if start_timestamp == -1:
        start_timestamp = current_timestamp - np.timedelta(100, 'Y')
    for x in range(len(data)):
        row_time = data.iloc[x]['timestamp']
        if row_time >= start_timestamp and row_time <= current_timestamp:
            available += data.iloc[x]['available']
            count +=1
        if row_time > current_timestamp:
            break

    if count:
        return available/count

    return 0


window = 60 # seconds FIXME TODO

def impute(df):
    df['available'] = 1
    missing_vals = pd.DataFrame(columns=df.columns)

    for x in range(1, len(df['timestamp'].values)):
        diff = (df['timestamp'].values[x] - df['timestamp'].values[x-1])/np.timedelta64(1, 's')#1000000000
        if diff > 60:
            num_rows_to_insert = int(diff/60) - 1
            available_userid = df.iloc[x]['user']
            available_timestamp = df.iloc[x]['timestamp']
            available_stress = df.iloc[x]['stress_probability']

            for y in range(num_rows_to_insert):
                imputed_timestamp = available_timestamp + np.timedelta64((y+1)*window, 's')
                new_row = [available_userid, imputed_timestamp, available_stress, 0]
                missing_vals.loc[len(missing_vals)] = new_row


    df_imputed = df.append(missing_vals)

    df_imputed = df_imputed.sort_values(by=['timestamp'])

    return df_imputed


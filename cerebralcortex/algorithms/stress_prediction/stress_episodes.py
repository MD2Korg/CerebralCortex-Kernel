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
import pickle


NOTSTRESS = "NOTSTRESS"
UNSURE = 'UNSURE'
YESSTRESS = 'YESSTRESS'
UNKNOWN = 'UNKNOWN'
NOTCLASSIFIED = 'NOTCLASSIFIED'


schema = StructType([
    StructField("user", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("stress_episodes", FloatType()),
])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def stress_episodes_estimation(data: object) -> object:
    # --- Constants definitions --- 
    smoothing_window = 3 # FIXME - 3 minutes
    macd_param_fast = 7;
    macd_param_slow = 19;
    macd_param_signal = 2;
    threshold_yes = 0.36;
    threshold_no = 0.36;

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
            #TODO

        if histogram_prev >=0 and histogram < 0:
            # Episode is in the middle, started decreasing
            episode_start_timestamp = get_episode_start_timestamp() 
            if episode_start_timestamp == -1:
                stress_episode_start.append((episode_start_timestamp, NOTCLASSIFIED))
                stress_episode_peak.append((stress_smoothed_list[c][0], NOTCLASSIFIED))
                stress_episode_classification.append((stress_smoothed_list[c][0], NOTCLASSIFIED))
            else:
                proportion_available = get_stress_proportion() # TODO 
                if proportion_availble < 0.5:
                    stress_episode_start.append((episode_start_timestamp, UNKNOWN))
                    stress_episode_peak.append((stress_smoothed_list[c][0], UNKNOWN))
                    stress_episode_classification.append((stress_smoothed_list[c][0], UNKNOWN))
                else:
                    historical_stress = get_historical_stress_values(episode_start_time)
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
                        else if stress_density <= threshold_no:
                            stress_episode_start.append((episode_start_timestamp, NOTSTRESS))
                            stress_episode_peak.append((stress_smoothed_list[c][0], NOTSTRESS))
                            stress_episode_classification.append((stress_smoothed_list[c][0], NOTSTRESS))
                        else:
                            stress_episode_start.append((episode_start_timestamp, UNSURE))
                            stress_episode_peak.append((stress_smoothed_list[c][0], UNSURE))
                            stress_episode_classification.append((stress_smoothed_list[c][0], UNSURE))
                            




    df = pd.DataFrame(index = np.arange(0, len(data['timestamp'].values)), columns=['user', 'timestamp', 'stress_episodes'])
    user = data['user'].values[0]
    for c in range(len(data['timestamp'].values)):
        ts = data['timestamp'].values[c]
        prob = predicted[c][1]
        df.loc[c] = [user, ts, prob]

    return df

def ewma(double x, double y, double alpha):
    return alpha * x + (1 - alpha) * y

def get_episode_start_timestamp():
    return -1 # TODO FIXME

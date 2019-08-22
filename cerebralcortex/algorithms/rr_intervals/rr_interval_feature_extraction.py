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

class Quality(Enum):
    ACCEPTABLE = 1
    UNACCEPTABLE = 0

def outlier_computation(valid_rr_interval_time: list,
                        valid_rr_interval_sample: list,
                        criterion_beat_difference: float):
    """
    This function implements the rr interval outlier calculation through comparison with the criterion
    beat difference and consecutive differences with the previous and next sample

    :param valid_rr_interval_time: A python array of rr interval time
    :param valid_rr_interval_sample: A python array of rr interval samples
    :param criterion_beat_difference: A threshold calculated from the RR interval data passed

    yields: The quality of each data point in the RR interval array
    """
    standard_rr_interval_sample = valid_rr_interval_sample[0]
    previous_rr_interval_quality = Quality.ACCEPTABLE

    for i in range(1, len(valid_rr_interval_sample) - 1):

        rr_interval_diff_with_last_good = abs(standard_rr_interval_sample - valid_rr_interval_sample[i])
        rr_interval_diff_with_prev_sample = abs(valid_rr_interval_sample[i - 1] - valid_rr_interval_sample[i])
        rr_interval_diff_with_next_sample = abs(valid_rr_interval_sample[i] - valid_rr_interval_sample[i + 1])

        if previous_rr_interval_quality == Quality.UNACCEPTABLE and rr_interval_diff_with_last_good < criterion_beat_difference:
            yield (valid_rr_interval_time[i], Quality.ACCEPTABLE)
            previous_rr_interval_quality = Quality.ACCEPTABLE
            standard_rr_interval_sample = valid_rr_interval_sample[i]

        elif previous_rr_interval_quality == Quality.UNACCEPTABLE and rr_interval_diff_with_last_good > criterion_beat_difference >= rr_interval_diff_with_prev_sample and rr_interval_diff_with_next_sample <= criterion_beat_difference:
            yield (valid_rr_interval_time[i], Quality.ACCEPTABLE)
            previous_rr_interval_quality = Quality.ACCEPTABLE
            standard_rr_interval_sample = valid_rr_interval_sample[i]

        elif previous_rr_interval_quality == Quality.UNACCEPTABLE and rr_interval_diff_with_last_good > criterion_beat_difference and (
                        rr_interval_diff_with_prev_sample > criterion_beat_difference or rr_interval_diff_with_next_sample > criterion_beat_difference):
            yield (valid_rr_interval_time[i], Quality.UNACCEPTABLE)
            previous_rr_interval_quality = Quality.UNACCEPTABLE

        elif previous_rr_interval_quality == Quality.ACCEPTABLE and rr_interval_diff_with_prev_sample <= criterion_beat_difference:
            yield (valid_rr_interval_time[i], Quality.ACCEPTABLE)
            previous_rr_interval_quality = Quality.ACCEPTABLE
            standard_rr_interval_sample = valid_rr_interval_sample[i]

        elif previous_rr_interval_quality == Quality.ACCEPTABLE and rr_interval_diff_with_prev_sample > criterion_beat_difference:
            yield (valid_rr_interval_time[i], Quality.UNACCEPTABLE)
            previous_rr_interval_quality = Quality.UNACCEPTABLE

        else:
            yield (valid_rr_interval_time[i], Quality.UNACCEPTABLE)





def compute_outlier(rr_intervals):
    """
    Reference - Berntson, Gary G., et al. "An approach to artifact identification: Application to heart period data."
    Psychophysiology 27.5 (1990): 586-598.

    :param ecg_rr: RR interval datastream

    :return: An annotated datastream specifying when the ECG RR interval datastream is acceptable
    """

    # print(1)

    valid_rr_interval_sample = [i[1] for i in rr_intervals if i[1] > .3 and i[1] < 2]
    valid_rr_interval_time = [i[0] for i in rr_intervals if i[1] > .3 and i[1] < 2]

    if not len(valid_rr_interval_sample):
        return  pd.DataFrame(columns=['timestamp', 'rr_interval'])

    valid_rr_interval_difference = abs(np.diff(valid_rr_interval_sample))
    # plt.plot(valid_rr_interval_time,valid_rr_interval_sample)
    # plt.show()
    # Maximum Expected Difference(MED)= 3.32* Quartile Deviation
    maximum_expected_difference = 4.5 * 0.5 * iqr(valid_rr_interval_difference)

    # Shortest Expected Beat(SEB) = Median Beat – 2.9 * Quartile Deviation
    # Minimal Artifact Difference(MAD) = SEB/ 3
    maximum_artifact_difference = (np.median(valid_rr_interval_sample) - 2.9 * .5 * iqr(
        valid_rr_interval_difference)) / 3

    # Midway between MED and MAD is considered
    criterion_beat_difference = (maximum_expected_difference + maximum_artifact_difference) / 2
    if criterion_beat_difference < .2:
        criterion_beat_difference = .2

    ecg_rr_quality_array = [(valid_rr_interval_time[0], valid_rr_interval_sample[0])]

    count = 1
    for data in outlier_computation(valid_rr_interval_time, valid_rr_interval_sample, criterion_beat_difference):
        if(data[1] == Quality.ACCEPTABLE):
            ecg_rr_quality_array.append((valid_rr_interval_time[count], valid_rr_interval_sample[count]))
        count += 1

    ecg_rr_quality_array.append((valid_rr_interval_time[-1], valid_rr_interval_sample[-1]))

    df = pd.DataFrame(ecg_rr_quality_array, columns=['timestamp', 'rr_interval'])
    return df






def lomb(time_stamps:List,
         samples:List,
         low_frequency: float,
         high_frequency: float):
    """
   : Lomb–Scargle periodogram implementation
    :param data: List[DataPoint]
    :param high_frequency: float
    :param low_frequency: float
    :return lomb-scargle pgram and frequency values
    """

    frequency_range = np.linspace(low_frequency, high_frequency, len(time_stamps))
    try:
        result = signal.lombscargle(time_stamps, samples, frequency_range)
        return result, frequency_range
    except:
        return np.array([]), np.array([])


def heart_rate_power(power: np.ndarray,
                     frequency: np.ndarray,
                     low_rate: float,
                     high_rate: float):
    """
    Compute Heart Rate Power for specific frequency range
    :param power: np.ndarray
    :param frequency: np.ndarray
    :param high_rate: float
    :param low_rate: float
    :return: sum of power for the frequency range
    """
    result_power = float(0.0)
    for i, value in enumerate(power):
        if low_rate <= frequency[i] <= high_rate:
            result_power += value
    return result_power



def rr_feature_computation(timestamp:list,
                            value:list,
                            low_frequency: float = 0.01,
                            high_frequency: float = 0.7,
                            low_rate_vlf: float = 0.0009,
                            high_rate_vlf: float = 0.04,
                            low_rate_hf: float = 0.15,
                            high_rate_hf: float = 0.4,
                            low_rate_lf: float = 0.04,
                            high_rate_lf: float = 0.15):
    """
    ECG Feature Implementation. The frequency ranges for High, Low and Very low heart rate variability values are
    derived from the following paper:
    'Heart rate variability: standards of measurement, physiological interpretation and clinical use'
    :param high_rate_lf: float
    :param low_rate_lf: float
    :param high_rate_hf: float
    :param low_rate_hf: float
    :param high_rate_vlf: float
    :param low_rate_vlf: float
    :param high_frequency: float
    :param low_frequency: float
    :param datastream: DataStream
    :param window_size: float
    :param window_offset: float
    :return: ECG Feature DataStreams
    """


    # perform windowing of datastream


    # initialize each ecg feature array

    rr_variance_data = []
    rr_mean_data = []
    rr_median_data = []
    rr_80percentile_data = []
    rr_20percentile_data = []
    rr_quartile_deviation_data = []
    rr_HF_data = []
    rr_LF_data = []
    rr_VLF_data = []
    rr_LF_HF_data = []
    rr_heart_rate_data = []

    # iterate over each window and calculate features


    reference_data = value

    rr_variance_data.append(np.var(reference_data))

    power, frequency = lomb(time_stamps=timestamp,samples=value,low_frequency=low_frequency, high_frequency=high_frequency)
    
    if power.size == 0 or frequency.size==0:
        return []

    rr_VLF_data.append(heart_rate_power(power, frequency, low_rate_vlf, high_rate_vlf))

    rr_HF_data.append(heart_rate_power(power, frequency, low_rate_hf, high_rate_hf))

    rr_LF_data.append(heart_rate_power(power,frequency,low_rate_lf,high_rate_lf))

    if heart_rate_power(power, frequency, low_rate_hf, high_rate_hf) != 0:
        lf_hf = float(heart_rate_power(power, frequency, low_rate_lf, high_rate_lf) / heart_rate_power(power,
                                                                                                       frequency,
                                                                                                       low_rate_hf,
                                                                                                       high_rate_hf))
        rr_LF_HF_data.append(lf_hf)
    else:
        rr_LF_HF_data.append(0)

    rr_mean_data.append(np.mean(reference_data))
    rr_median_data.append(np.median(reference_data))
    rr_quartile_deviation_data.append((0.5*(np.percentile(reference_data, 75) - np.percentile(reference_data,25))))
    rr_heart_rate_data.append(np.median(60000/reference_data))

    return [rr_variance_data[0], rr_VLF_data[0], rr_HF_data[0], rr_LF_data[0], rr_LF_HF_data[0],\
           rr_mean_data[0], rr_median_data[0], rr_quartile_deviation_data[0], rr_heart_rate_data[0],\
           np.percentile(value,80),np.percentile(value,20)]


def get_windows(data):
    window_col,ts_col = [],[]
    rr_interval = data.sort_values(by=['timestamp'])
    st = (rr_interval['timestamp'].values[0].astype('int64')/1e9)*1000
    et = (rr_interval['timestamp'].values[-1].astype('int64')/1e9)*1000
    ts_array = np.arange(st,et,60000)
    
    x = [(i.astype('int64')/1e9)*1000 for i in rr_interval['timestamp'].values]
    y = [i for i in rr_interval['rr_interval'].values]
    #tmp_ts = np.array(x)
    #tmp_rri = np.array(y)

    tmp_rri = np.zeros((len(x),2))
    for c in range(len(x)):
        tmp_rri[c][0] = x[c]
        tmp_rri[c][1] = y[c]

    for t in ts_array:
        #index = np.where((tmp_ts >= t) & (tmp_ts <= t+60000))[0]
        index = np.where((tmp_rri[:,0]>=t)&(tmp_rri[:,0]<=t+60000))[0]

        if len(index)>100 or len(index)<20:
            continue

        window_col.append(tmp_rri[index,:])
        #window_col.append([[tmp_ts[i], tmp_rri[i]] for i in index])
        ts_col.append(t)
    return window_col,ts_col

def combine_data(window_col):
    feature_matrix = np.zeros((0,11))
    for i,item in enumerate(window_col):
        feature = rr_feature_computation(item[:,0],item[:,1])
        if not len(feature):
            continue
        feature_matrix = np.concatenate((feature_matrix,np.array(feature).reshape(-1,11)))
    return feature_matrix



schema = StructType([
    StructField("user", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("rr_feature", ArrayType(FloatType())),
])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def rr_interval_feature_extraction(raw_data: object) -> object:
    winsor_limit = 0.1 #FIXME - this must be passed or configurable

    to_filter = []
    raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])
    raw_data['rr_interval'] = raw_data['rr_interval'].apply(lambda x: float(x))
    raw_data['rr_interval'] /= 1000.0
    for i in range(len(raw_data['timestamp'].values)):
        to_filter.append([raw_data['timestamp'].values[i], raw_data['rr_interval'].values[i]])

    data = compute_outlier(to_filter)
    if not len(data):
        return pd.DataFrame(columns=['user', 'timestamp', 'rr_feature'])

    data.insert(0,'user',raw_data['user'].values[0])
    # FIXME TODO
    
    mean = data['rr_interval'].mean()
    std = data['rr_interval'].std()

    data['rr_interval'] = (data['rr_interval'] - mean)/std

    window_col, ts_col = get_windows(data)
    print('AB'*50)
    print(window_col)
    X = combine_data(window_col)
    print(X)
    for k in range(X.shape[1]):
        if not len(X[:,k]):
            continue
        X[:,k] = winsorize(X[:,k],limits=[winsor_limit,winsor_limit])


    df = pd.DataFrame(index = np.arange(0, len(X)), columns=['user', 'timestamp', 'rr_feature'])
    user = data['user'].values[0]
    for c in range(len(X)):
        df.loc[c] = [user, np.datetime64(int(ts_col[c]), 'ms'), X[c]]

    return df


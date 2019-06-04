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
import scipy.signal as signal
import datetime



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
    result = signal.lombscargle(time_stamps, samples, frequency_range)
    return result, frequency_range


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






def ecg_feature_computation(timestamp:list,
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


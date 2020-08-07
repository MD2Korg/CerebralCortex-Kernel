# Copyright (c) 2020, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

import math

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.group import GroupedData
from pyspark.sql.types import *
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


def complementary_filter(ds, freq: int = 16, accelerometer_x: str = "accelerometer_x",
                         accelerometer_y: str = "accelerometer_y", accelerometer_z: str = "accelerometer_z",
                         gyroscope_x: str = "gyroscope_x", gyroscope_y: str = "gyroscope_y",
                         gyroscope_z: str = "gyroscope_z"):
    """
    Compute complementary filter on gyro and accel data.

    Args:
        ds (DataStream ): Non-Windowed/grouped dataframe
        freq (int): frequency of accel/gryo. Assumption is that frequency is equal for both gyro and accel.
        accelerometer_x (str): name of the column
        accelerometer_y (str): name of the column
        accelerometer_z (str): name of the column
        gyroscope_x (str): name of the column
        gyroscope_y (str): name of the column
        gyroscope_z (str): name of the column
    """
    dt = 1.0 / freq  # 1/16.0;
    M_PI = math.pi;
    hpf = 0.90;
    lpf = 0.10;


    window = Window.partitionBy(ds._data['user']).orderBy(ds._data['timestamp'])

    data = ds._data.withColumn("thetaX_accel",
                               ((F.atan2(-F.col(accelerometer_z), F.col(accelerometer_y)) * 180 / M_PI)) * lpf) \
        .withColumn("roll",
                    (F.lag("thetaX_accel").over(window) + F.col(gyroscope_x) * dt) * hpf + F.col("thetaX_accel")).drop(
        "thetaX_accel") \
        .withColumn("thetaY_accel",
                    ((F.atan2(-F.col(accelerometer_x), F.col(accelerometer_z)) * 180 / M_PI)) * lpf) \
        .withColumn("pitch",
                    (F.lag("thetaY_accel").over(window) + F.col(gyroscope_y) * dt) * hpf + F.col("thetaY_accel")).drop(
        "thetaY_accel") \
        .withColumn("thetaZ_accel",
                    ((F.atan2(-F.col(accelerometer_y), F.col(accelerometer_x)) * 180 / M_PI)) * lpf) \
        .withColumn("yaw",
                    (F.lag("thetaZ_accel").over(window) + F.col(gyroscope_z) * dt) * hpf + F.col("thetaZ_accel")).drop(
        "thetaZ_accel")

    return DataStream(data=data.dropna(), metadata=Metadata())


def compute_zero_cross_rate(ds, exclude_col_names: list = [],
                            feature_names=['zero_cross_rate']):
    """
    Compute statistical features.

    Args:
        ds (DataStream ): Windowed/grouped dataframe
        exclude_col_names list(str): name of the columns on which features should not be computed
        feature_names list(str): names of the features. Supported features are ['mean', 'median', 'stddev', 'variance', 'max', 'min', 'skew',
                     'kurt', 'sqr', 'zero_cross_rate'
        windowDuration (int): duration of a window in seconds
        slideDuration (int): slide duration of a window
        groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
        startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided


    Returns:
        DataStream object
    """
    exclude_col_names.extend(["timestamp", "localtime", "user", "version"])

    data = ds._data.drop(*exclude_col_names)

    df_column_names = data.columns

    basic_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("user", StringType()),
        StructField("version", IntegerType()),
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType())
    ])

    features_list = []
    for cn in df_column_names:
        for sf in feature_names:
            features_list.append(StructField(cn + "_" + sf, FloatType(), True))

    features_schema = StructType(basic_schema.fields + features_list)

    def calculate_zero_cross_rate(series):
        """
        How often the signal changes sign (+/-)
        """
        series_mean = np.mean(series)
        series = [v - series_mean for v in series]
        zero_cross_count = (np.diff(np.sign(series)) != 0).sum()
        return zero_cross_count / len(series)

    @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
    def get_features_udf(df):
        results = []
        timestamp = df['timestamp'].iloc[0]
        localtime = df['localtime'].iloc[0]
        user = df['user'].iloc[0]
        version = df['version'].iloc[0]
        start_time = timestamp
        end_time = df['timestamp'].iloc[-1]

        df.drop(exclude_col_names, axis=1, inplace=True)
        if "zero_cross_rate" in feature_names:
            df_zero_cross_rate = df.apply(calculate_zero_cross_rate)
            df_zero_cross_rate.index += '_zero_cross_rate'
            results.append(df_zero_cross_rate)

        output = pd.DataFrame(pd.concat(results)).T

        basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time]],
                                columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time'])
        return basic_df.assign(**output)

    # check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception(
            "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")

    data = ds._data.apply(get_features_udf)
    return DataStream(data=data, metadata=Metadata())


def compute_FFT_features(ds, exclude_col_names: list = [],
                         feature_names=["fft_centroid", 'fft_spread', 'spectral_entropy', 'fft_flux',
                                            'spectral_falloff']):
    """
    Transforms data from time domain to frequency domain.

    Args:
        exclude_col_names list(str): name of the columns on which features should not be computed
        feature_names list(str): names of the features. Supported features are fft_centroid, fft_spread, spectral_entropy, spectral_entropy_old, fft_flux, spectral_falloff
        windowDuration (int): duration of a window in seconds
        slideDuration (int): slide duration of a window
        groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
        startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided


    Returns:
        DataStream object with all the existing data columns and FFT features
    """
    eps = 0.00000001

    exclude_col_names.extend(["timestamp", "localtime", "user", "version"])

    data = ds._data.drop(*exclude_col_names)

    df_column_names = data.columns

    basic_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("user", StringType()),
        StructField("version", IntegerType()),
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType())
    ])

    features_list = []
    for cn in df_column_names:
        for sf in feature_names:
            features_list.append(StructField(cn + "_" + sf, FloatType(), True))

    features_schema = StructType(basic_schema.fields + features_list)

    def stSpectralCentroidAndSpread(X, fs):
        """Computes spectral centroid of frame (given abs(FFT))"""
        ind = (np.arange(1, len(X) + 1)) * (fs / (2.0 * len(X)))

        Xt = X.copy()
        Xt = Xt / Xt.max()
        NUM = np.sum(ind * Xt)
        DEN = np.sum(Xt) + eps

        # Centroid:
        C = (NUM / DEN)

        # Spread:
        S = np.sqrt(np.sum(((ind - C) ** 2) * Xt) / DEN)

        # Normalize:
        C = C / (fs / 2.0)
        S = S / (fs / 2.0)

        return (C, S)

    def stSpectralFlux(X, Xprev):
        """
        Computes the spectral flux feature of the current frame
        ARGUMENTS:
            X:        the abs(fft) of the current frame
            Xpre:        the abs(fft) of the previous frame
        """
        # compute the spectral flux as the sum of square distances:

        sumX = np.sum(X + eps)
        sumPrevX = np.sum(Xprev + eps)
        F = np.sum((X / sumX - Xprev / sumPrevX) ** 2)

        return F

    def stSpectralRollOff(X, c, fs):
        """Computes spectral roll-off"""

        totalEnergy = np.sum(X ** 2)
        fftLength = len(X)
        Thres = c * totalEnergy
        # Ffind the spectral rolloff as the frequency position where the respective spectral energy is equal to c*totalEnergy
        CumSum = np.cumsum(X ** 2) + eps
        [a, ] = np.nonzero(CumSum > Thres)
        if len(a) > 0:
            mC = np.float64(a[0]) / (float(fftLength))
        else:
            mC = 0.0
        return (mC)

    def stSpectralEntropy(X, numOfShortBlocks=10):
        """Computes the spectral entropy"""
        L = len(X)  # number of frame samples
        Eol = np.sum(X ** 2)  # total spectral energy

        subWinLength = int(np.floor(L / numOfShortBlocks))  # length of sub-frame
        if L != subWinLength * numOfShortBlocks:
            X = X[0:subWinLength * numOfShortBlocks]

        subWindows = X.reshape(subWinLength, numOfShortBlocks,
                               order='F').copy()  # define sub-frames (using matrix reshape)
        s = np.sum(subWindows ** 2, axis=0) / (Eol + eps)  # compute spectral sub-energies
        En = -np.sum(s * np.log2(s + eps))  # compute spectral entropy

        return En

    def spectral_entropy(data, sampling_freq, bands=None):

        psd = np.abs(np.fft.rfft(data)) ** 2
        psd /= np.sum(psd)  # psd as a pdf (normalised to one)

        if bands is None:
            power_per_band = psd[psd > 0]
        else:
            freqs = np.fft.rfftfreq(data.size, 1 / float(sampling_freq))
            bands = np.asarray(bands)

            freq_limits_low = np.concatenate([[0.0], bands])
            freq_limits_up = np.concatenate([bands, [np.Inf]])

            power_per_band = [np.sum(psd[np.bitwise_and(freqs >= low, freqs < up)])
                              for low, up in zip(freq_limits_low, freq_limits_up)]

            power_per_band = power_per_band[power_per_band > 0]

        return -np.sum(power_per_band * np.log2(power_per_band))

    def fourier_features_pandas_udf(data, frequency: float = 16.0):

        Fs = frequency  # the sampling freq (in Hz)
        results = []
        # fourier transforms!
        # data_fft = abs(np.fft.rfft(data))

        X = abs(np.fft.fft(data))
        nFFT = int(len(X) / 2) + 1

        X = X[0:nFFT]  # normalize fft
        X = X / len(X)

        if "fft_centroid" or "fft_spread" in feature_names:
            C, S = stSpectralCentroidAndSpread(X, Fs)  # spectral centroid and spread
            if "fft_centroid" in feature_names:
                results.append(C)
            if "fft_spread" in feature_names:
                results.append(S)
        if "spectral_entropy" in feature_names:
            se = stSpectralEntropy(X)  # spectral entropy
            results.append(se)
        if "spectral_entropy_old" in feature_names:
            se_old = spectral_entropy(X, frequency)  # spectral flux
            results.append(se_old)
        if "fft_flux" in feature_names:
            flx = stSpectralFlux(X, X.copy())  # spectral flux
            results.append(flx)
        if "spectral_folloff" in feature_names:
            roff = stSpectralRollOff(X, 0.90, frequency)  # spectral rolloff
            results.append(roff)
        return pd.Series(results)

    @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
    def get_fft_features(df):
        timestamp = df['timestamp'].iloc[0]
        localtime = df['localtime'].iloc[0]
        user = df['user'].iloc[0]
        version = df['version'].iloc[0]
        start_time = timestamp
        end_time = df['timestamp'].iloc[-1]

        df.drop(exclude_col_names, axis=1, inplace=True)

        df_ff = df.apply(fourier_features_pandas_udf)
        df3 = df_ff.T
        pd.set_option('display.max_colwidth', -1)

        df3.columns = feature_names

        # multiple rows to one row
        output = df3.unstack().to_frame().sort_index(level=1).T
        output.columns = [f'{j}_{i}' for i, j in output.columns]

        basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time]],
                                columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time'])
        # df.insert(loc=0, columns=, value=basic_cols)
        return basic_df.assign(**output)

    # check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception(
            "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")

    data = ds._data.apply(get_fft_features)
    return DataStream(data=data, metadata=Metadata())

from typing import List

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
# from pyspark.sql.functions import pandas_udf,PandasUDFType
from pyspark.sql.types import StructType

from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


def compute_corr_mse_accel_gyro(self, exclude_col_names: list = [],
                                accel_column_names: list = ['accelerometer_x', 'accelerometer_y', 'accelerometer_z'],
                                gyro_column_names: list = ['gyroscope_y', 'gyroscope_x', 'gyroscope_z'],
                                windowDuration: int = None,
                                slideDuration: int = None,
                                groupByColumnName: List[str] = [], startTime=None):
    """
    Compute correlation and mean standard error of accel and gyro sensors

    Args:
        exclude_col_names list(str): name of the columns on which features should not be computed
        accel_column_names list(str): name of accel data column
        gyro_column_names list(str): name of gyro data column
        windowDuration (int): duration of a window in seconds
        slideDuration (int): slide duration of a window
        groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
        startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided


    Returns:
        DataStream object with all the existing data columns and FFT features
    """
    feature_names = ["ax_ay_corr", 'ax_az_corr', 'ay_az_corr', 'gx_gy_corr', 'gx_gz_corr',
                     'gy_gz_corr', 'ax_ay_mse', 'ax_az_mse', 'ay_az_mse', 'gx_gy_mse', 'gx_gz_mse', 'gy_gz_mse']

    exclude_col_names.extend(["timestamp", "localtime", "user", "version"])

    data = self._data.drop(*exclude_col_names)

    basic_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("user", StringType()),
        StructField("version", IntegerType()),
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType())
    ])

    features_list = []
    for fn in feature_names:
        features_list.append(StructField(fn, FloatType(), True))

    features_schema = StructType(basic_schema.fields + features_list)

    @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
    def get_corr_mse_features_udf(df):
        timestamp = df['timestamp'].iloc[0]
        localtime = df['localtime'].iloc[0]
        user = df['user'].iloc[0]
        version = df['version'].iloc[0]
        start_time = timestamp
        end_time = df['timestamp'].iloc[-1]

        ax_ay_corr = df[accel_column_names[0]].corr(df[accel_column_names[1]])
        ax_az_corr = df[accel_column_names[0]].corr(df[accel_column_names[2]])
        ay_az_corr = df[accel_column_names[1]].corr(df[accel_column_names[2]])
        gx_gy_corr = df[gyro_column_names[0]].corr(df[gyro_column_names[1]])
        gx_gz_corr = df[gyro_column_names[0]].corr(df[gyro_column_names[2]])
        gy_gz_corr = df[gyro_column_names[1]].corr(df[gyro_column_names[2]])

        ax_ay_mse = ((df[accel_column_names[0]] - df[accel_column_names[1]]) ** 2).mean()
        ax_az_mse = ((df[accel_column_names[0]] - df[accel_column_names[2]]) ** 2).mean()
        ay_az_mse = ((df[accel_column_names[1]] - df[accel_column_names[2]]) ** 2).mean()
        gx_gy_mse = ((df[accel_column_names[0]] - df[accel_column_names[1]]) ** 2).mean()
        gx_gz_mse = ((df[accel_column_names[0]] - df[accel_column_names[2]]) ** 2).mean()
        gy_gz_mse = ((df[accel_column_names[1]] - df[accel_column_names[2]]) ** 2).mean()

        basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time, ax_ay_corr,
                                  ax_az_corr, ay_az_corr, gx_gy_corr, gx_gz_corr, gy_gz_corr, ax_ay_mse, ax_az_mse,
                                  ay_az_mse, gx_gy_mse, gx_gz_mse, gy_gz_mse]],
                                columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time',
                                         "ax_ay_corr", 'ax_az_corr', 'ay_az_corr', 'gx_gy_corr', 'gx_gz_corr',
                                         'gy_gz_corr', 'ax_ay_mse', 'ax_az_mse', 'ay_az_mse', 'gx_gy_mse',
                                         'gx_gz_mse', 'gy_gz_mse'])
        return basic_df

    data = self.compute(get_corr_mse_features_udf, windowDuration=windowDuration, slideDuration=slideDuration,
                        groupByColumnName=groupByColumnName, startTime=startTime)
    return DataStream(data=data._data, metadata=Metadata())

def compute_fourier_features(self, exclude_col_names: list = [],
                             feature_names=["fft_centroid", 'fft_spread', 'spectral_entropy',
                                            'spectral_entropy_old', 'fft_flux',
                                            'spectral_falloff'], windowDuration: int = None,
                             slideDuration: int = None,
                             groupByColumnName: List[str] = [], startTime=None):
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

    data = self._data.drop(*exclude_col_names)

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
        # split column into multiple columns
        # df3 = pd.DataFrame(df_ff.values.tolist(), index=df_ff.index)
        # print("**"*50)
        # print(type(df), type(df_ff), type(df3))
        # print(df)
        # print(df_ff)
        # print(df_ff.values.tolist())
        # print(df3)
        # print("**" * 50)
        # print("FEATURE-NAMES", feature_names)
        df3.columns = feature_names

        # multiple rows to one row
        output = df3.unstack().to_frame().sort_index(level=1).T
        output.columns = [f'{j}_{i}' for i, j in output.columns]

        basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time]],
                                columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time'])
        # df.insert(loc=0, columns=, value=basic_cols)
        return basic_df.assign(**output)

    return self.compute(get_fft_features, windowDuration=windowDuration, slideDuration=slideDuration,
                        groupByColumnName=groupByColumnName, startTime=startTime)
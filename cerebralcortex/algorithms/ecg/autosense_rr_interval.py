# Copyright (c) 2020, MD2K Center of Excellence
# All rights reserved.
# Md Azim Ullah (mullah@memphis.edu)
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

from enum import Enum

import numpy as np
import pandas as pd
from ecgdetectors import Detectors
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType
from scipy.stats import iqr

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata


def get_rr_interval(ecg_data,Fs=64):
    """


    Args:
        ecg_data (DataStream):
        Fs (int):

    Returns:
        DataStream - timestamp, localtime, user, version ....
    """
    stream_name = 'org.md2k.autosense.ecg.rr'

    class Quality(Enum):
        ACCEPTABLE = 1
        UNACCEPTABLE = 0

    def get_metadata():
        """
        generate metadata for the stream

        Returns:
            MetaData object
        """
        stream_metadata = Metadata()
        stream_metadata.set_name(stream_name).set_description("ECG RR interval in milliseconds") \
            .add_input_stream(ecg_data.metadata.get_name()) \
            .add_dataDescriptor(
            DataDescriptor().set_name("rr").set_type("float") \
                .set_attribute("description","rr interval")) \
            .add_module(
            ModuleMetadata().set_name("ecg rr interval") \
                .set_attribute("url","http://md2k.org/") \
                .set_attribute('algorithm','pan-tomkins').set_attribute('unit','ms').set_author("Md Azim Ullah", "mullah@memphis.edu"))
        return stream_metadata

    def outlier_computation(valid_rr_interval_time: list,
                            valid_rr_interval_sample: list,
                            criterion_beat_difference: float):
        """
        This function implements the rr interval outlier calculation through comparison with the criterion
        beat difference and consecutive differences with the previous and next sample

        Args:
            valid_rr_interval_time:  A python array of rr interval time
            valid_rr_interval_sample: A python array of rr interval samples
            criterion_beat_difference: A threshold calculated from the RR interval data passed

        Yields:
            The quality of each data point in the RR interval array
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


    def compute_outlier_ecg(ecg_ts,ecg_rr):
        """
        Reference - Berntson, Gary G., et al. "An approach to artifact identification: Application to heart period data."
        Psychophysiology 27.5 (1990): 586-598.

        Args:
            ecg_ts (timestamp):
            ecg_rr (..): RR interval

        Returns:
            An annotated datastream specifying when the ECG RR interval datastream is acceptable
        """


        valid_rr_interval_sample = [i for i in ecg_rr if i > .3 and i < 2]
        valid_rr_interval_time = [ecg_ts[i] for i in range(len(ecg_ts)) if ecg_rr[i] > .3 and ecg_rr[i] < 2]
        valid_rr_interval_difference = abs(np.diff(valid_rr_interval_sample))

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

        ecg_rr_quality_array = [(valid_rr_interval_time[0], Quality.ACCEPTABLE)]

        for data in outlier_computation(valid_rr_interval_time, valid_rr_interval_sample, criterion_beat_difference):
            ecg_rr_quality_array.append(data)
        ecg_rr_quality_array.append((valid_rr_interval_time[-1], Quality.ACCEPTABLE))
        return ecg_rr_quality_array

    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("version", IntegerType()),
        StructField("user", StringType()),
        StructField("rr", FloatType())
    ])
    detectors = Detectors(Fs)

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('org.md2k.autosense.ecg.quality', 'get_rr_interval', stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def ecg_r_peak(data):
        """

        Args:
            data:

        Returns:

        """
        if data.shape[0]>1000:
            data = data.sort_values('timestamp').reset_index(drop=True)
            index_all = np.array(list(range(data.shape[0])))
            rpeaks = detectors.hamilton_detector(data['ecg'].values)
            rpeaks = np.array(rpeaks)
            if len(rpeaks)<3:
                return pd.DataFrame([],columns=['timestamp','localtime','version','user','rr'])
            rpeak_ts = 1000*data['time'].values[rpeaks]
            ecg_rr_ts = rpeak_ts[1:]
            ecg_rr_val = np.diff(rpeak_ts)
            index_all = index_all[rpeaks][1:]

            index = np.where((ecg_rr_val>=400)&(ecg_rr_val<=2000))[0]
            if len(index)<3:
                return pd.DataFrame([],columns=['timestamp', 'localtime', 'version', 'user', 'rr'])

            ecg_rr_ts = ecg_rr_ts[index]
            ecg_rr_val = ecg_rr_val[index]
            index_all = index_all[index]

            outlier = compute_outlier_ecg(ecg_rr_ts/1000,ecg_rr_val/1000)
            index = []
            for ind,tup in enumerate(outlier):
                if tup[1]==Quality.ACCEPTABLE:
                    index.append(ind)

            if len(index)<3:
                return pd.DataFrame([],columns=['timestamp','localtime','version','user','rr'])
            index = np.array(index)
            ecg_rr_val = ecg_rr_val[index]
            index_all = index_all[index]

            data = data.iloc[data.index[list(index_all)]]

            data['rr'] = list(np.float64(ecg_rr_val))

            data = data[['timestamp','localtime','version','user','rr']]
            return data
        else:
            return pd.DataFrame([],columns=['timestamp','localtime','version','user','rr'])

    ecg_data = ecg_data.withColumn('time',F.col('timestamp').cast('double'))
    ecg_data = ecg_data.filter(F.col('quality')=='acceptable')
    ecg_rr = ecg_data.compute(ecg_r_peak,windowDuration=600,startTime='0 seconds')
    ecg_rr.metadata = get_metadata()
    return ecg_rr


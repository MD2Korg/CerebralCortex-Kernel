# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
# Anand
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

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType

from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata


def compute_stress_episodes(ecg_stress_probability, macd_param_fast = 7, macd_param_slow = 19, macd_param_signal = 2, threshold_stressed = 0.36, threshold_not_stressed= 0.36):
    """
    Compute stress episodes using MACD

    Args:
        ecg_stress_probability (DataStream):
        macd_param_fast (int):
        macd_param_slow (int):
        macd_param_signal (into):
        threshold_stressed (float):
        threshold_not_stressed (float):

    Returns:
        DataStream: with a column stress_episodes
    """

    NOTSTRESS = "NOTSTRESSED"
    UNSURE = 'UNSURE'
    YESSTRESS = 'STRESSED'
    UNKNOWN = 'UNKNOWN'
    UNCLASSIFIED = 'UNCLASSIFIED'
    stream_name = "org.md2k.autosense.ecg.stress_episodes"

    def get_metadata():
        stream_metadata = Metadata()
        stream_metadata.set_name(stream_name).set_description("Stress episodes computed using MACD formula.") \
            .add_input_stream(ecg_stress_probability.metadata.get_name()) \
            .add_dataDescriptor(DataDescriptor().set_name("timestamp").set_type("datetime")) \
            .add_dataDescriptor(DataDescriptor().set_name("localtime").set_type("datetime")) \
            .add_dataDescriptor(DataDescriptor().set_name("version").set_type("int")) \
            .add_dataDescriptor(DataDescriptor().set_name("user").set_type("string")) \
            .add_dataDescriptor(
            DataDescriptor().set_name("stress_probability").set_type("float")) \
            .add_dataDescriptor(
            DataDescriptor().set_name("stress_episode").set_type("string").set_attribute("description", \
                                                                              "stress episodes calculated using MACD")) \
            .add_module(
            ModuleMetadata().set_name("cerebralcortex.algorithm.stress_prediction.stress_episodes.compute_stress_episodes")
                .set_attribute("url", "http://md2k.org/").set_author(
                "Anandatirtha Nandugudi", "dev@md2k.org"))
        return stream_metadata

    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("stress_probability", FloatType()),
        StructField("stress_episode", StringType()),
        StructField("user", StringType()),
        StructField("version", IntegerType())
    ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('org.md2k.autosense.ecg.stress.probability.imputed', 'stress_episodes_estimation', stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def stress_episodes_estimation(stress_data: object) -> object:
        """
        smooth stress probabilities and use MACD to label stress episodes

        Args:
            stress_data (pandas.df):

        Returns:
            pandas.df
        """
        smoothing_window = 3 # FIXME - 3 minutes
        user_id = stress_data.iloc[0].user
        # Smooth the stress values
        stress_smoothed_list = []

        for indx in range(len(stress_data['stress_probability'].values)):
            if indx<2:
                smoothed_stress = stress_data['stress_probability'].values[indx]
            else:
                smoothed_stress = (stress_data['stress_probability'].values[indx] + \
                                 stress_data['stress_probability'].values[indx-1] + \
                                 stress_data['stress_probability'].values[indx-2]) / smoothing_window

            stress_smoothed_list.append((stress_data['timestamp'].values[indx], smoothed_stress, stress_data['localtime'].values[indx]))

        # EMA = Exponential Moving Average
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

        for indx in range(len(stress_smoothed_list)):
            ema_fast_prev = ema_fast_list[-1][1]
            ema_fast_current = stress_smoothed_list[indx][1]
            ema_fast = ewma(ema_fast_current, ema_fast_prev, 2.0/(macd_param_fast + 1))
            ema_fast_list.append((stress_smoothed_list[indx][0], ema_fast))

            ema_slow_prev = ema_slow_list[-1][1]
            ema_slow_current = stress_smoothed_list[indx][1]
            ema_slow = ewma(ema_slow_current, ema_slow_prev, 2.0/(macd_param_slow + 1))
            ema_slow_list.append((stress_smoothed_list[indx][0], ema_slow))

            macd_prev = ema_fast_prev - ema_slow_prev
            macd_current = ema_fast_current - ema_slow_current
            ema_signal_prev = ema_signal_list[-1][1]
            ema_signal = ewma(macd_current, macd_prev, 2.0/(macd_param_signal + 1))
            ema_signal_list.append((stress_smoothed_list[indx][0], ema_signal))

            histogram_prev =  macd_prev - ema_signal_prev
            histogram = macd_current - ema_signal
            histogram_list.append((stress_smoothed_list[indx][0], histogram))

            if histogram_prev <=0 and histogram > 0:
                # Episode has ended, started increasing again
                start_timestamp = None;
                peak_timestamp = None;
                end_timestamp = stress_smoothed_list[indx][0]
                stress_class = None
                if len(stress_episode_start):
                    start_timestamp = stress_episode_start[-1][0]

                if len(stress_episode_classification):
                    peak_timestamp = stress_episode_classification[-1][0]
                    stress_class = stress_episode_classification[-1][1]

                if stress_class:
                    #TODO - Handle this?????
                    stress_episode_timestamps = []
                    stress_episode_timestamps.append(start_timestamp)
                    stress_episode_timestamps.append(peak_timestamp)
                    stress_episode_timestamps.append(end_timestamp)
                    stress_episode_timestamps.append(stress_class)
                    stress_episode_intervals.append(stress_episode_timestamps)

            if histogram_prev >=0 and histogram < 0:
                # Episode is in the middle, started decreasing
                episode_start_timestamp = get_episode_start_timestamp(stress_episode_classification, histogram_list, stress_smoothed_list[indx][0])
                if episode_start_timestamp is None:
                    stress_episode_start.append((episode_start_timestamp, UNCLASSIFIED))
                    stress_episode_peak.append((stress_smoothed_list[indx][0], UNCLASSIFIED))
                    stress_episode_classification.append([stress_smoothed_list[indx][0], stress_smoothed_list[indx][2], stress_smoothed_list[indx][1], UNCLASSIFIED])
                else:
                    proportion_available = get_proportion_available(stress_data, episode_start_timestamp, stress_smoothed_list[indx][0])
                    if proportion_available < 0.5:
                        stress_episode_start.append((episode_start_timestamp, UNKNOWN))
                        stress_episode_peak.append((stress_smoothed_list[indx][0], UNKNOWN))
                        stress_episode_classification.append([stress_smoothed_list[indx][0], stress_smoothed_list[indx][2], stress_smoothed_list[indx][1], UNKNOWN])
                    else:
                        historical_stress = get_historical_values_timestamp_based(stress_smoothed_list, episode_start_timestamp, stress_smoothed_list[indx][0])
                        if not len(historical_stress):
                            stress_episode_start.append((episode_start_timestamp, UNKNOWN))
                            stress_episode_peak.append((stress_smoothed_list[indx][0], UNKNOWN))
                            stress_episode_classification.append([stress_smoothed_list[indx][0], stress_smoothed_list[indx][2], stress_smoothed_list[indx][1], UNKNOWN])
                        else:
                            cumu_sum = 0.0
                            for hs in historical_stress:
                                cumu_sum += hs[1]
                            stress_density = cumu_sum / len(historical_stress)
                            if stress_density >= threshold_stressed:
                                stress_episode_start.append((episode_start_timestamp, YESSTRESS))
                                stress_episode_peak.append((stress_smoothed_list[indx][0], YESSTRESS))
                                stress_episode_classification.append([stress_smoothed_list[indx][0], stress_smoothed_list[indx][2], stress_smoothed_list[indx][1], YESSTRESS])
                            elif stress_density <= threshold_not_stressed:
                                stress_episode_start.append((episode_start_timestamp, NOTSTRESS))
                                stress_episode_peak.append((stress_smoothed_list[indx][0], NOTSTRESS))
                                stress_episode_classification.append([stress_smoothed_list[indx][0], stress_smoothed_list[indx][2], stress_smoothed_list[indx][1], NOTSTRESS])
                            else:
                                stress_episode_start.append((episode_start_timestamp, UNSURE))
                                stress_episode_peak.append((stress_smoothed_list[indx][0], UNSURE))
                                stress_episode_classification.append([stress_smoothed_list[indx][0], stress_smoothed_list[indx][2], stress_smoothed_list[indx][1], UNSURE])

        df = pd.DataFrame(stress_episode_classification, columns=['timestamp', 'localtime', 'stress_probability', 'stress_episode'])
        df['user'] = user_id
        df['version'] = 1
        return df

    def ewma(current:int, previous:int, alpha:int)->float:
        """
        compute exponential weighted moving average

        Args:
            current (int): current stress probability value
            previous (int): previous stress probability value
            alpha (int):

        Returns:
            float
        """
        return alpha * current + (1 - alpha) * previous

    def get_episode_start_timestamp(stress_episode_classification, histogram_list, currenttime):
        """
        Get start time of a stress episode

        Args:
            stress_episode_classification:
            histogram_list:
            currenttime:

        Returns:

        """
        timestamp_prev = None
        if len(stress_episode_classification) >= 3:
            timestamp_prev = stress_episode_classification[-3][0]
        elif len(stress_episode_classification) == 2:
            timestamp_prev = stress_episode_classification[-2][0]
        elif len(stress_episode_classification) == 1:
            timestamp_prev = stress_episode_classification[-1][0]

        histogram_history =  get_historical_values_timestamp_based(histogram_list, timestamp_prev, currenttime)

        if len(histogram_history) <= 1:
            return None

        for x in range(len(histogram_history)-2 , -1, -1):
            if histogram_history[x][1] <= 0:
                return histogram_history[x+1][0]

        return histogram_history[0][0]


    def get_historical_values_timestamp_based(data, start_timestamp, currenttime):
        """

        Args:
            data:
            start_timestamp:
            currenttime:

        Returns:

        """
        toreturn = []
        starttime = start_timestamp
        if starttime == None:
            starttime = currenttime - np.timedelta64(100*365*24*3600, 's') # approx 100 year to approximate -1
        for c in data:
            if c[0] >= starttime and c[0] <= currenttime:
                toreturn.append(c)
            if c[0] > currenttime:
                break

        return toreturn


    def get_proportion_available(data, st, current_timestamp):
        """
        compute the ratio of (detected + imputed stress episodes)/detected stress episodes

        Args:
            data:
            st:
            current_timestamp:

        Returns:

        """
        count = 0
        available = 0
        start_timestamp = st
        if start_timestamp is None:
            start_timestamp = current_timestamp
        for x in range(len(data)):
            row_time = data.iloc[x]['timestamp']
            if row_time >= start_timestamp and row_time <= current_timestamp:
                available += data.iloc[x]['imputed']
                count +=1
            if row_time > current_timestamp:
                break

        if count:
            return available/count

        return 0

    data = ecg_stress_probability._data.groupby(['user', 'version']).apply(stress_episodes_estimation)
    return DataStream(data=data, metadata=get_metadata())
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

import math

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import DataDescriptor


def normalize_features(data,
                       index_of_first_order_feature =2,
                       lower_percentile=20,
                       higher_percentile=99,
                       minimum_minutes_in_day=60,
                       no_features=11,
                       epsilon = 1e-8,
                       input_feature_array_name='features'):
    """

    Args:
        data:
        index_of_first_order_feature:
        lower_percentile:
        higher_percentile:
        minimum_minutes_in_day:
        no_features:
        epsilon:
        input_feature_array_name:

    Returns:

    """
    data_day = data.withColumn('day',F.date_format('localtime','yyyyMMdd'))
    stream_metadata = data.metadata
    stream_metadata \
        .add_input_stream(data.metadata.get_name()) \
        .add_dataDescriptor(
        DataDescriptor()
            .set_name("features_normalized")
            .set_type("array")
            .set_attribute("description","All features normalized daywise"))
    data_day = data_day.withColumn('features_normalized',F.col(input_feature_array_name))
    if 'window' in data.columns:
        data_day = data_day.withColumn('start',F.col('window').start).withColumn('end',F.col('window').end).drop(*['window'])
    schema = data_day._data.schema

    def weighted_avg_and_std(values, weights):
        """
        Return the weighted average and standard deviation.

        values, weights -- Numpy ndarrays with the same shape.
        """
        average = np.average(values, weights=weights)
        # Fast and numerically precise:
        variance = np.average((values-average)**2, weights=weights)
        return average, math.sqrt(variance)

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('org.md2k.autosense.ecg.features', 'normalize_features', "org.md2k.autosense.ecg.normalized.features", ['user', 'timestamp'], ['user', 'timestamp'])
    def normalize_features(data):
        """


        Args:
            data:

        Returns:

        """
        if len(data)<minimum_minutes_in_day:
            return pd.DataFrame([], columns=data.columns)
        quals1 = np.array([1] * data.shape[0])
        feature_matrix = np.array(list(data[input_feature_array_name])).reshape(-1, no_features)
        ss = np.repeat(feature_matrix[:,index_of_first_order_feature],np.int64(np.round(100*quals1)))
        rr_70th = np.percentile(ss,lower_percentile)
        rr_95th = np.percentile(ss,higher_percentile)
        index = np.where((feature_matrix[:,index_of_first_order_feature]>rr_70th)&(feature_matrix[:,index_of_first_order_feature]<rr_95th))[0]
        for i in range(feature_matrix.shape[1]):
            m,s = weighted_avg_and_std(feature_matrix[index,i], quals1[index])
            s+=epsilon
            feature_matrix[:,i]  = (feature_matrix[:,i] - m)/s
        data['features_normalized']  = list([np.array(b) for b in feature_matrix])
        return data

    data_normalized = data_day._data.groupby(['user','day','version']).apply(normalize_features)
    if 'window' in data.columns:
        data_normalized = data_normalized.withColumn('window',F.struct('start', 'end')).drop(*['start','end','day'])
    else:
        data_normalized = data_normalized.drop(*['day'])
    features = DataStream(data=data_normalized,metadata=stream_metadata)
    return features

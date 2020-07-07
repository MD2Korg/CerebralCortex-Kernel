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
from cerebralcortex.algorithms.ecg.data_quality import ecg_quality
from cerebralcortex.algorithms.ecg.rr_interval import get_rr_interval
from cerebralcortex.algorithms.ecg.hrv_features import get_hrv_features
from cerebralcortex.algorithms.utils.feature_normalization import normalize_features
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, DoubleType,MapType, StringType,ArrayType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import numpy as np
import pandas as pd
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata
import pickle


def compute_stress_probability(stress_features_normalized,
                               model_path='.',
                               feature_index=None):
    stress_features_normalized = stress_features_normalized.withColumn('start',F.col('window').start)
    stress_features_normalized = stress_features_normalized.withColumn('end',F.col('window').end).drop('window')
    stress_features_normalized = stress_features_normalized.withColumn('stress_probability',F.lit(1).cast('double'))
    schema = stress_features_normalized._data.schema
    ecg_model = pickle.load(open(model_path,'rb'))

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_stress_prob(data):
        if data.shape[0]>0:
            features = []
            for i in range(data.shape[0]):
                features.append(np.array(data['features_normalized'].values[i]))
            features = np.nan_to_num(np.array(features))
            if feature_index is None:
                features = features[:,feature_index]
            probs = ecg_model.predict_proba(features)[:,1]
            data['stress_probability'] = probs
            return data
        else:
            return pd.DataFrame([],columns=data.columns)

    ecg_stress_likelihoods = stress_features_normalized.compute(get_stress_prob,windowDuration=6000,startTime='0 seconds')
    ecg_stress_final = ecg_stress_likelihoods.select('timestamp', F.struct('start', 'end').alias('window'), 'localtime','stress_probability','user','version')
    return ecg_stress_final




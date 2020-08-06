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

import pickle

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata


def compute_stress_probability(stress_features_normalized,
                               model_path='.',
                               feature_index=None):
    """


    Args:
        stress_features_normalized:
        model_path:
        feature_index:

    Returns:

    """
    stream_name = 'org.md2k.autosense.ecg.stress.probability'
    def get_metadata():
        stream_metadata = Metadata()
        stream_metadata.set_name(stream_name).set_description("stress likelihood computed from ECG") \
            .add_input_stream(stress_features_normalized.metadata.get_name()) \
            .add_dataDescriptor(
            DataDescriptor().set_name("stress_probability")
                .set_type("double").set_attribute("description","stress likelihood computed from ECG only model")
                .set_attribute("threshold","0.47")) \
            .add_dataDescriptor(
            DataDescriptor().set_name("window")
                .set_type("struct")
                .set_attribute("description", "window start and end time in UTC")
                .set_attribute('start', 'start of 1 minute window')
                .set_attribute('end','end of 1 minute window')) \
            .add_module(
            ModuleMetadata().set_name("ECG Stress Model")
                .set_attribute("url", "http://md2k.org/")
                .set_attribute('algorithm','cStress')
                .set_attribute('unit','ms').set_author("Md Azim Ullah", "mullah@memphis.edu"))
        return stream_metadata

    stress_features_normalized = stress_features_normalized.withColumn('start',F.col('window').start)
    stress_features_normalized = stress_features_normalized.withColumn('end',F.col('window').end).drop('window')
    stress_features_normalized = stress_features_normalized.withColumn('stress_probability',F.lit(1).cast('double'))
    schema = stress_features_normalized._data.schema
    ecg_model = pickle.load(open(model_path,'rb'))

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('"org.md2k.autosense.ecg.normalized.features"', 'get_hrv_features', stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def get_stress_prob(data):
        """


        Args:
            data:

        Returns:

        """
        if data.shape[0]>0:
            features = []
            for i in range(data.shape[0]):
                features.append(np.array(data['features_normalized'].values[i]))
            features = np.nan_to_num(np.array(features))
            if feature_index is not None:
                features = features[:,feature_index]
            probs = ecg_model.predict_proba(features)[:,1]
            data['stress_probability'] = probs
            return data
        else:
            return pd.DataFrame([],columns=data.columns)

    ecg_stress_likelihoods = stress_features_normalized.compute(get_stress_prob,windowDuration=6000,startTime='0 seconds')
    ecg_stress_final = ecg_stress_likelihoods.select('timestamp', F.struct('start', 'end').alias('window'), 'localtime','stress_probability','user','version')
    ecg_stress_final.metadata = get_metadata()
    return ecg_stress_final




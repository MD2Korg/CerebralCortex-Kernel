# Copyright (c) 2020, MD2K Center of Excellence
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


from cerebralcortex.algorithms.ecg.autosense_data_quality import ecg_autosense_data_quality
from cerebralcortex.algorithms.ecg.autosense_rr_interval import get_rr_interval
from cerebralcortex.algorithms.ecg.hrv_features import get_hrv_features
from cerebralcortex.algorithms.stress_prediction.ecg_stress import compute_stress_probability
from cerebralcortex.algorithms.stress_prediction.stress_episodes import compute_stress_episodes
from cerebralcortex.algorithms.stress_prediction.stress_imputation import forward_fill_data, impute_stress_likelihood
from cerebralcortex.algorithms.utils.feature_normalization import normalize_features
from cerebralcortex.core.datatypes.datastream import DataStream

def stress_from_ecg(ecg_data:DataStream, labels, sensor_name:str="autosense", Fs:int=64, model_path="./model/stress_ecg_final.p"):
    """
    Compute stress episodes from ecg timeseries data

    Args:
        ecg_data (DataStream): ecg data
        sensor_name (str): name of the sensor used to collect ecg data. Currently supports 'autosense' only
        Fs (int): frequency of sensor data

    Returns:
        DataStream: stress episodes
    """

    ###                          Stress computation pipeline

    # Compute data quality
    ecg_data_with_quality = ecg_autosense_data_quality(ecg_data,sensor_name=sensor_name,Fs=Fs)

    # Compute RR intervals
    ecg_rr = get_rr_interval(ecg_data_with_quality,Fs=Fs)

    # Compute HRV features
    stress_features = get_hrv_features(ecg_rr)

    # Normalize features
    stress_features_normalized = normalize_features(stress_features,input_feature_array_name='features')

    # Compute stress probability
    ecg_stress_probability = compute_stress_probability(stress_features_normalized,model_path=model_path)

    ecg_stress_probability  = ecg_stress_probability.sort("window")

    # Forward fill and impute stress data
    ecg_stress_probability_forward_filled = forward_fill_data(ecg_stress_probability)
    ecg_stress_probability_imputed = impute_stress_likelihood(ecg_stress_probability_forward_filled)


    # Compute stress episodes
    stress_episodes = compute_stress_episodes(ecg_stress_probability=ecg_stress_probability_imputed)

    return stress_episodes

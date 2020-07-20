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
import argparse
from pyspark.sql import functions as F
from cerebralcortex.kernel import Kernel
from cerebralcortex.algorithms.ecg.autosense_data_quality import ecg_autosense_data_quality
from cerebralcortex.algorithms.ecg.autosense_rr_interval import get_rr_interval
from cerebralcortex.algorithms.ecg.hrv_features import get_hrv_features
from cerebralcortex.algorithms.utils.feature_normalization import normalize_features
from cerebralcortex.algorithms.stress_prediction.ecg_stress import compute_stress_probability
from cerebralcortex.algorithms.stress_prediction.stress_imputation import forward_fill_data, impute_stress_likelihood
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata
import pickle
import numpy as np




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECG Stress Calculation")
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=False,default="/Users/ali/IdeaProjects/CerebralCortex-2.0/conf/")
    parser.add_argument('-a', '--ecg_stream_name', help='Input ECG Stream Name', required=False,default="ecg--org.md2k.autosense--autosense_chest--chest")
    parser.add_argument('-s', '--study_name', help='Study Name', required=False,default="rice")
    parser.add_argument('-f', '--frequency', help='ECG Sampling Frequency', required=False,default="64")
    parser.add_argument('-p', '--path', help='Stress Model Path', required=False,default='/Users/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/markers/ecg_stress/model/stress_ecg_final.p')
    parser.add_argument('-n', '--sensor_name', help='Sensor Type', required=False,default='autosense')

    # parse arguements
    args = vars(parser.parse_args())
    config_dir = str(args["config_dir"]).strip()
    ecg_stream_name = str(args["ecg_stream_name"]).strip()
    study_name = str(args["study_name"]).strip()
    Fs = int(str(args["frequency"]).strip())
    model_path = str(args["path"]).strip()
    sensor_name = str(args["sensor_name"]).strip()

    # create CC object
    CC = Kernel(config_dir, study_name=study_name)

    # get stream data
    ecg_data = CC.get_stream(ecg_stream_name)

    # Stress computation pipline
    ecg_data_with_quality = ecg_autosense_data_quality(ecg_data,sensor_name=sensor_name,Fs=Fs)
    ecg_rr = get_rr_interval(ecg_data_with_quality,Fs=Fs)
    stress_features = get_hrv_features(ecg_rr)
    stress_features_normalized = normalize_features(stress_features,input_feature_array_name='features')
    ecg_stress_probability = compute_stress_probability(stress_features_normalized,model_path=model_path)
    ecg_stress_probability_forward_filled = forward_fill_data(ecg_stress_probability)
    ecg_stress_probability_imputed = impute_stress_likelihood(ecg_stress_probability_forward_filled)
    ecg_stress_probability_imputed.show()
    #CC.save_stream(ecg_stress_probability_imputed,overwrite=True)
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
from cerebralcortex.algorithms.ecg.data_quality import ecg_quality
from cerebralcortex.algorithms.ecg.rr_interval import get_rr_interval
from cerebralcortex.algorithms.ecg.hrv_features import get_hrv_features
from cerebralcortex.algorithms.utils.feature_normalization import normalize_features
from cerebralcortex.algorithms.stress_prediction.ecg_stress import compute_stress_probability
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata
import pickle
import numpy as np

def get_metadata(stream_name = 'org.md2k.autosense.ecg.stress.probability'):
    stream_metadata = Metadata()
    stream_metadata.set_name(stream_name).set_description("stress likelihood computed from ECG") \
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECG Stress Calculation")
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=False,default="/Users/ali/IdeaProjects/CerebralCortex-2.0/conf/")
    parser.add_argument('-a', '--ecg_stream_name', help='Input ECG Stream Name', required=False,default="ecg--org.md2k.autosense--autosense_chest--chest")
    parser.add_argument('-s', '--study_name', help='Study Name', required=False,default="rice")
    parser.add_argument('-f', '--frequency', help='ECG Sampling Frequency', required=False,default="64")
    parser.add_argument('-p', '--path', help='Stress Model Path', required=False,default='./model/stress_ecg_final.p')
    parser.add_argument('-n', '--sensor_name', help='Sensor Type', required=False,default='autosense')
    parser.add_argument('-o', '--output_stream_name', help='Final Stress Stream Name', required=False,default='org.md2k.autosense.ecg.stress.probability')


    args = vars(parser.parse_args())
    config_dir = str(args["config_dir"]).strip()
    ecg_stream_name = str(args["ecg_stream_name"]).strip()
    study_name = str(args["study_name"]).strip()
    Fs = int(str(args["frequency"]).strip())
    model_path = str(args["path"]).strip()
    sensor_name = str(args["sensor_name"]).strip()
    output_stream_name = str(args["output_stream_name"]).strip()

    CC = Kernel(config_dir, study_name=study_name)
    ecg_data = CC.get_stream(ecg_stream_name)
    ecg_data_with_quality = ecg_quality(ecg_data,sensor_name=sensor_name,Fs=Fs)
    ecg_rr = get_rr_interval(ecg_data_with_quality,Fs=Fs)
    stress_features = get_hrv_features(ecg_rr)
    feature_names = ['var','iqr','mean','median','80th','20th','heartrate','vlf','lf','hf','lfhf']
    stress_features = stress_features.withColumn('features',F.array([F.col(i) for i in feature_names]))
    stress_features_normalized = normalize_features(stress_features,input_feature_array_name='features')
    ecg_stress_probability = compute_stress_probability(stress_features_normalized,model_path=model_path)
    ecg_stress_probability.metadata = get_metadata(stream_name=output_stream_name)

    ecg_stress_probability.show()
    #CC.save_stream(ecg_stress_probability,overwrite=True)
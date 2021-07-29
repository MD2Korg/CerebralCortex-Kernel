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

from cerebralcortex.kernel import Kernel
from cerebralcortex.markers.ecg_stress.stress_from_ecg import stress_from_ecg

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECG Stress Calculation")
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=False,default="../../conf/")
    parser.add_argument('-a', '--ecg_stream_name', help='Input ECG Stream Name', required=False,default="wesad.chest.ecg")
    parser.add_argument('-s', '--study_name', help='Study Name', required=False,default="wesad")
    parser.add_argument('-f', '--frequency', help='ECG Sampling Frequency', required=False,default="700")
    parser.add_argument('-p', '--path', help='Stress Model Path', required=False,default='../markers/ecg_stress/model/stress_ecg_final.p')
    parser.add_argument('-n', '--sensor_name', help='Sensor Type', required=False,default='respiban')

    # parse arguments
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

    label = CC.get_stream("wesad.label")
    stress_episodes = stress_from_ecg(ecg_data, label, sensor_name=sensor_name, Fs=Fs, model_path=model_path)

    # show results
    stress_episodes.show(60)

    # Store results
    # CC.save_stream(clusterz)
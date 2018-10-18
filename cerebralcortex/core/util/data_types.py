# Copyright (c) 2017, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

import json
import pickle
from cerebralcortex.core.datatypes.datapoint import DataPoint


def convert_sample_complete(sample):
    if isinstance(sample, str) and "\x00" in sample:
        sample = sample.replace("\x00", "")

    if isinstance(sample, tuple):
        values = list(sample)
    elif isinstance(sample, list):
        values = sample
    else:
        try:
            values = json.loads(sample)
            if not isinstance(values, list) and not isinstance(values, dict):
                values = [values]
        except:
            try:
                values = eval(sample)
                if isinstance(values, tuple):
                    values = list(values)
            except:
                try:
                    values = [float(sample)]
                except:
                    try:
                        values = list(map(float, values.split(',')))
                    except:
                        values = [sample]
    return values


def convert_sample_short(sample):
    try:
        values = json.loads(sample)
        if not isinstance(values, list) and not isinstance(values, dict):
            values = [values]
            return  values
    except:
        try:
            if isinstance(sample, str) and "," in sample:
                return list(
                    map(float, sample[1:-1].split(',')))
        except:
            try:
                return [float(sample)]
            except:
                return [sample]
    return sample


def convert_sample_type(sample: str) -> object:
    """
    Convert a string to string-list format.
    :param sample:
    :return:
    """
    if not sample.startswith("[") or not sample.startswith("{"):
        return "[%s]" % sample


def convert_sample(sample, stream_name):
    if "RAW--org.md2k." in stream_name or "CU_AUDIO_FEATURE--" in stream_name:
        return sample
    try:
        if isinstance(sample, str):
            sample = sample.strip()

        if sample[0]=="[" or sample[0]=="{":
            return json.loads(sample)
        elif isinstance(sample, str) and "," in sample:
            tmp = []
            for val in sample.split(','):
                val = str(val).strip()
                try:
                    tmp.append(float(val))
                except:
                    tmp.append(val)
            return tmp
        else:
            try:
                return [float(sample)]
            except:
                return [sample]
    except:
        return sample


def serialize_obj(datapoints:DataPoint):
    res = pickle.dumps(datapoints)
    return res


def deserialize_obj(picked_obj):
    res = pickle.loads(picked_obj)
    return res
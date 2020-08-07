# Copyright (c) 2019, MD2K Center of Excellence
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

import pathlib
import unittest
import warnings

from cerebralcortex import Kernel
from cerebralcortex.algorithms.glucose.glucose_variability_metrics import glucose_var
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata


class TestDataframeUDF(unittest.TestCase):

    def test_00(self):
        warnings.simplefilter("ignore")
        config_filepath = "./../../conf/"

        # create sample_data directory. Note: make sure this path is same as the filesystem path in cerebralcortex.yml
        pathlib.Path("./sample_data/").mkdir(parents=True, exist_ok=True)

        self.study_name = "dbdp"
        self.CC = Kernel(config_filepath, study_name=self.study_name)
        spark = self.CC.sparkSession

        df = spark.read.load("./sample_data/data/study=dbdp/stream=cgm_glucose_variability_metrics/")
        df.show()
        ds = DataStream(data=df, metadata=Metadata())

        results = glucose_var(ds)
        results.show()




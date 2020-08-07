# Copyright (c) 2017, MD2K Center of Excellence
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
import pickle

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType

schema = StructType([
    StructField("user", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("stress_probability", FloatType()),
])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def stress_prediction(data: object) -> object:

    num_rows = len(data['rr_feature'].values)

    fm = np.zeros((num_rows, 11))
    for c in range(num_rows):
        for k in range(11):
            fm[c][k] = data['rr_feature'].values[c][k]

    clf_ecg = pickle.load(open('/home/a/stress_classifier/classifier_for_ecg.p','rb'))
    predicted = clf_ecg.predict_proba(fm)

    df = pd.DataFrame(index = np.arange(0, len(data['timestamp'].values)), columns=['user', 'timestamp', 'stress_probability'])
    user = data['user'].values[0]
    for c in range(len(data['timestamp'].values)):
        ts = data['timestamp'].values[c]
        prob = predicted[c][1]
        df.loc[c] = [user, ts, prob]

    return df


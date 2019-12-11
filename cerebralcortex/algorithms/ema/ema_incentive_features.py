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

import json

import pandas as pd


from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType

schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("localtime", TimestampType()),
    StructField("user", StringType()),
    StructField("version", IntegerType()),
    StructField("incentive", FloatType()),
    StructField("total_incentive", FloatType()),
    StructField("ema_id", StringType()),
    StructField("data_quality", FloatType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def get_ema_incentive_features(user_data):
    all_vals = []
    for index, row in user_data.iterrows():
        ema = row["incentive"]
        if not isinstance(ema, dict):
            ema = json.loads(ema)

        incentive = ema["incentive"]
        total_incentive = ema["totalIncentive"]
        ema_id = ema["emaId"]
        data_quality = ema["dataQuality"]


        all_vals.append([row["timestamp"],row["localtime"], row["user"],1,incentive,total_incentive,ema_id,data_quality])

    return pd.DataFrame(all_vals,columns=['timestamp','localtime', 'user', 'version','incentive','total_incentive','ema_id','data_quality'])

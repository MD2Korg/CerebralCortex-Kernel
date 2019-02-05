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


def get_or_create_sc(type="sparkContext", name="CerebralCortex-Kernal"):
    """
    get or create spark context

    Args:
        type (str): type (sparkContext, SparkSessionBuilder, sparkSession, sqlContext). (default="sparkContext")
        name (str): spark app name (default="CerebralCortex-Kernal")

    Returns:

    """
    from pyspark.sql import SQLContext
    from pyspark.sql import SparkSession

    ss = SparkSession.builder
    if name:
        ss.appName(name)

    ss.config("spark.streaming.backpressure.enabled", True)
    ss.config("spark.streaming.backpressure.initialRate", 1)
    ss.config("spark.streaming.kafka.maxRatePerPartition", 2)

    sparkSession = ss.getOrCreate()

    sc = sparkSession.sparkContext
    sc.setLogLevel("FATAL")

    sqlContext = SQLContext(sc)
    if type=="SparkSessionBuilder":
        return sc
    elif type=="sparkContext":
        return sc
    elif type=="sparkSession":
        return ss
    elif type=="sqlContext":
        return sqlContext
    else:
        raise ValueError("Unknown type.")

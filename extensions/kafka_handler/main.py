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

import sys
from core import CC
from core.kafka_consumer import spark_kafka_consumer
from core.kafka_to_cc_storage_engine import kafka_to_db
from pyspark.streaming import StreamingContext
from core.kafka_producer import kafka_file_to_json_producer

# Kafka Consumer Configs
batch_duration = 5  # seconds
ssc = StreamingContext(CC.getOrCreateSC(type="sparkContext"), batch_duration)
CC.getOrCreateSC(type="sparkContext").setLogLevel("WARN")
broker = "localhost:9092"  # multiple brokers can be passed as comma separated values
consumer_group_id = "md2k-test"

data_path = sys.argv[1]
if (data_path[-1] != '/'):
    data_path += '/'

kafka_files_stream = spark_kafka_consumer(["filequeue"], ssc, broker, consumer_group_id)
kafka_files_stream.foreachRDD(lambda rdd: kafka_file_to_json_producer(rdd, data_path))

# kafka_processed_stream = spark_kafka_consumer(["processed_stream"], ssc, broker, consumer_group_id)
# kafka_processed_stream.foreachRDD(kafka_to_db)


ssc.start()
ssc.awaitTermination()

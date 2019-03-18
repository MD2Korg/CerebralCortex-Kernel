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
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
from kafka import KafkaProducer

from cerebralcortex.core.messaging_manager.kafka_handler import KafkaHandler


class MessagingQueue(KafkaHandler):
    def __init__(self, CC: object, auto_offset_reset: str="largest"):
        """
        Messaging queue module support pub/sub system in CerebralCortex

        Args:
            CC (CerebralCortex): cerebralcortex class object
            auto_offset_reset (str): smallest (start of the topic) OR largest (end of a topic) (default="largest")
        """
        self.config = CC.config
        self.CC = CC

        if CC.config["messaging_service"]=="none":
            raise Exception("Messaging service is disabled (none) in cerebralcortex.yml. Please update configs.")


        self.ping_kafka = self.config["kafka"]["ping_kafka"]
        self.consumer_group_id = self.config["kafka"]["consumer_group_id"]

        try:
            ping_kafka = int(self.ping_kafka)
        except:
            raise Exception("ping_kafka value can only be an integer. Please check data_ingestion.yml")

        self.ssc = StreamingContext(self.CC.sparkContext, ping_kafka)

        if self.config["messaging_service"]!="none" and "kafka" in self.config and self.config['messaging_service']=="kafka":
            self.broker = str(self.config['kafka']['host']) +":"+str(self.config['kafka']['port'])
            self.auto_offset_reset= auto_offset_reset

            self.producer = KafkaProducer(bootstrap_servers=str(self.broker), api_version=(0,10),
                                          value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          compression_type='gzip')

            self.consumer = KafkaConsumer(bootstrap_servers=str(self.broker), api_version=(0,10),
                                          auto_offset_reset=self.auto_offset_reset)

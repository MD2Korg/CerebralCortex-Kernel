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
from pyspark.streaming.kafka import KafkaUtils, KafkaDStream, TopicAndPartition


class KafkaHandler():

    def produce_message(self, topic: str, msg: str):
        """
        Publish a message on kafka message queue

        Args:
            topic (str): name of the kafka topic
            msg (dict): message that needs to published on kafka
        Returns:
            bool: True if successful. In case of failure, it returns an Exception message.
        Raises:
            ValueError: topic and message parameters cannot be empty or None.
            Exception: Error publishing message. Topic: topic_name - error-message

        """

        if not topic and not msg:
            raise ValueError("topic and message parameters cannot be empty or None.")
        try:
            self.producer.send(topic, msg)
            self.producer.flush()
            return True
        except Exception as e:
            raise Exception("Error publishing message. Topic: "+str(topic)+" - "+str(e))

    def subscribe_to_topic(self, topic: str)-> dict:
        """
        Subscribe to kafka topic as a consumer

        Args:
            topic (str): name of the kafka topic
        Yields:
             dict: kafka message
        Raises:
            ValueError: Topic parameter is missing.
        """
        if not topic:
            raise ValueError("Topic parameter is missing.")

        self.consumer.subscribe(topic)
        for message in self.consumer: #TODO: this is a test-code.
            yield json.loads(message.value.decode('utf8'))

    def create_direct_kafka_stream(self, kafka_topic: str, ssc) -> KafkaDStream:
        """
        Create a direct stream to kafka topic. Supports only one topic at a time

        Args:
            kafka_topic: kafka topic to create stream against

        Raises:
             Exception: if direct stream cannot be created.

        Todo:
            Enable logging of errors
        """
        try:
            offsets = self.CC.get_kafka_offsets(kafka_topic)
            kafka_topic = [kafka_topic]

            if bool(offsets):
                fromOffset = {}
                for offset in offsets:
                    offset_start = offset["offset_start"]
                    topic_partition = offset["topic_partition"]
                    topic = offset["topic"]

                    topicPartion = TopicAndPartition(topic, int(topic_partition))
                    fromOffset[topicPartion] = int(offset_start)

                return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                     {"metadata.broker.list": self.broker,
                                                      "group.id": self.consumer_group_id}, fromOffsets=fromOffset)
            else:
                offset_reset = "smallest"  # smallest OR largest
                return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                     {"metadata.broker.list": self.broker, "auto.offset.reset": offset_reset,
                                                      "group.id": self.consumer_group_id})
        except Exception as e:
            print(e)

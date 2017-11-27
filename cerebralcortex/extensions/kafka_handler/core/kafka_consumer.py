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

from pyspark.streaming.kafka import KafkaUtils, KafkaDStream, TopicAndPartition


def spark_kafka_consumer(kafka_topic: str, ssc, broker, consumer_group_id, CC) -> KafkaDStream:
    """
    supports only one topic at a time
    :param kafka_topic:
    :return:
    """
    try:
        offsets = CC.get_kafka_offsets(kafka_topic[0])

        if bool(offsets):
            fromOffset = {}
            for offset in offsets:
                offset_start = offset["offset_start"]
                topic_partition = offset["topic_partition"]
                topic = offset["topic"]

                topicPartion = TopicAndPartition(topic, int(topic_partition))
                fromOffset[topicPartion] = int(offset_start)

            return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                 {"metadata.broker.list": broker,
                                                  "group.id": consumer_group_id}, fromOffsets=fromOffset)
        else:
            offset_reset = "smallest"  # smallest OR largest
            return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                 {"metadata.broker.list": broker, "auto.offset.reset": offset_reset,
                                                  "group.id": consumer_group_id})
    except Exception as e:
        print(e)

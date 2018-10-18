# Copyright (c) 2018, MD2K Center of Excellence
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

class KafkaHandler():

    def produce_message(self, topic: str, msg: dict):
        """
        Produce a message on Kafka topic
        :param topic:
        :param msg:
        """

        if not topic and not msg:
            raise ValueError("Topic and Message are required parameters.")
        try:
            self.producer.send(topic, msg)
            self.producer.flush()
        except Exception as e:
            raise Exception("Error publishing message. Topic: "+str(topic)+" - "+str(e))

    def subscribe_to_topic(self, topic: str)-> dict:
        """
        Subscribe to a topic on Kafka to consume messages
        :param topic:
        :param auto_offset_reset - smallest (start of the topic) OR largest (end of a topic)
        :return: Kafka message
        :rtype: dict
        """
        if not topic:
            raise ValueError("Topic parameter is missing.")

        self.consumer.subscribe(topic)
        for message in self.consumer: #TODO: this is a test-code.
            yield json.loads(message.value.decode('utf8'))

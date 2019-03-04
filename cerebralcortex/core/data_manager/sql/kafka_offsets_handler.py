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
from typing import List

class KafkaOffsetsHandler:

    def store_or_update_Kafka_offset(self, topic: str, topic_partition: str, offset_start: str, offset_until: str)->bool:
        """
        Store or Update kafka topic offsets. Offsets are used to track what messages have been processed.

        Args:
            topic (str): name of the kafka topic
            topic_partition (str): partition number
            offset_start (str): starting of offset
            offset_until (str): last processed offset
        Raises:
            ValueError: All params are required.
            Exception: Cannot add/update kafka offsets because ERROR-MESSAGE
        Returns:
            bool: returns True if offsets are add/updated or throws an exception.

        """
        if not topic and not topic_partition and not offset_start and not offset_until:
            raise ValueError("All params are required.")
        try:
            qry = "REPLACE INTO " + self.kafkaOffsetsTable + " (topic, topic_partition, offset_start, offset_until) VALUES(%s, %s, %s, %s)"
            vals = str(topic), str(topic_partition), str(offset_start), json.dumps(offset_until)
            self.execute(qry, vals, commit=True)
            return True
        except Exception as e:
            raise Exception("Cannot add/update kafka offsets because "+str(e))

    def get_kafka_offsets(self, topic: str) -> List[dict]:
        """
        Get last stored kafka offsets

        Args:
            topic (str): kafka topic name

        Returns:
            list[dict]: list of kafka offsets. This method will return empty list if topic does not exist and/or no offset is stored for the topic.
        Raises:
            ValueError: Topic name cannot be empty/None
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_kafka_offsets("live-data")
            >>> [{"id","topic", "topic_partition", "offset_start", "offset_until", "offset_update_time"}]
        """
        if not topic:
            raise ValueError("Topic name cannot be empty/None")
        results = []
        qry = "SELECT * from " + self.kafkaOffsetsTable + " where topic = %(topic)s  order by id DESC"
        vals = {'topic': str(topic)}
        rows = self.execute(qry, vals)
        if rows:
            for row in rows:
                results.append(row)
            return results
        else:
            return []

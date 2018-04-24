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

import datetime
from typing import List
from uuid import UUID

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.metadata_manager.metadata import DataDescriptor, ExecutionContext


class DataStream:
    def __init__(self,
                 identifier: UUID = None,
                 owner: UUID = None,
                 name: UUID = None,
                 data_descriptor: List[DataDescriptor] = None,
                 execution_context: ExecutionContext = None,
                 annotations: List = None,
                 stream_type: str = None,
                 start_time: datetime = None,
                 end_time: datetime = None,
                 data: List[DataPoint] = None,
                 stream_timezone=None
                 ):
        """
        DataStream object contains the list of DataPoint objects and metadata linked to it.
        :param identifier:
        :param owner:
        :param name:
        :param data_descriptor:
        :param execution_context:
        :param annotations:
        :param stream_type:
        :param start_time:
        :param end_time:
        :param data:
        :param stream_timezone:
        """
        self._identifier = identifier
        self._owner = owner
        self._name = name
        self._data_descriptor = data_descriptor
        self._datastream_type = stream_type
        self._execution_context = execution_context
        self._annotations = annotations
        self._start_time = start_time
        self._end_time = end_time
        self._data = data
        self._stream_timezone = stream_timezone

    def find_annotation_references(self, identifier: int = None, name: str = None):
        result = self._annotations
        found = False

        if identifier:
            found = True
            result = [a for a in result if a.stream_identifier == identifier]

        if name:
            found = True
            result = [a for a in result if a.name == name]

        if not found:
            return []

        return result

    @property
    def identifier(self):
        return self._identifier

    @property
    def owner(self):
        return self._owner

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def star_time(self):
        return self._start_time

    @star_time.setter
    def start_time(self, val):
        self._start_time = val

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, val):
        self._end_time = val

    @property
    def annotations(self):
        return self._annotations

    @annotations.setter
    def annotations(self, value):
        self._annotations = value

    @property
    def data_descriptor(self):
        return self._data_descriptor

    @data_descriptor.setter
    def data_descriptor(self, value):
        self._data_descriptor = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def datastream_type(self):
        return self._datastream_type

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        result = []
        for dp in value:
            result.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time, offset=dp.offset, sample=dp.sample))
        self._data = result

    # TODO- cannot use it due to circular dependencies. Moving it to CC class
    # def filter(self, annotation_stream_name: uuid, annotation: str, start_time: datetime, end_time: datetime) -> List[
    #     DataPoint]:
    #     """
    #     This method maps datastream to derived annotation stream and returns a List of Datapoints
    #     :param annotation_stream_name:
    #     :param annotation:
    #     :param start_time:
    #     :param end_time:
    #     :return:
    #     """
    # annotation_stream_id = Metadata.get_annotation_id(self.identifier, annotation_stream_name)
    # return SqlData.get_annotation_stream(annotation_stream_id, self.identifier, annotation, start_time, end_time)

    @classmethod
    def from_datastream(cls, input_streams: List):
        result = cls(owner=input_streams[0].owner)

        # TODO: Something with provenance tracking from datastream list

        return result

    def __str__(self):
        return "Stream(" + ', '.join(map(str, [self._identifier,
                                               self._owner,
                                               self._name,
                                               self._data_descriptor,
                                               self._datastream_type,
                                               self._execution_context,
                                               self._annotations,
                                               self._data]))

    def __repr__(self):
        return "Stream(" + ', '.join(map(str, [self._identifier,
                                               self._owner,
                                               self._name,
                                               self._data_descriptor,
                                               self._datastream_type,
                                               self._execution_context,
                                               self._annotations,
                                               self._data]))

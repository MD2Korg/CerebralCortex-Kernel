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


import uuid
from datetime import datetime
from typing import List

from cerebralcortex.core.datatypes.datapoint import DataPoint


class BlueprintStorage():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def read_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                  end_time: datetime = None, localtime: bool = True) -> List[DataPoint]:
        """
        Read data from a NoSQL storage
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        # TODO: implement your own storage layer to read data
        pass

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def write_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data to NoSQL storage.
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """

        # TODO: implement your own storage layer to write data
        pass

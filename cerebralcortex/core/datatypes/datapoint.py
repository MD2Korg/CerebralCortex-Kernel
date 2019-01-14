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

from datetime import datetime
from typing import Any


class DataPoint:
    def __init__(self,
                 start_time: datetime = None,
                 end_time: datetime = None,
                 offset: str = None,
                 sample: Any = None):
        """
        DataPoint is the lowest data representations entity in CerebralCortex.
        :param start_time:
        :param end_time:
        :param offset: in milliseconds
        :param sample:
        """
        self._start_time = start_time
        self._end_time = end_time
        self._offset = offset
        self._sample = sample

    @property
    def sample(self):
        return self._sample

    @sample.setter
    def sample(self, val):
        self._sample = val

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, val):
        self._start_time = val

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, val):
        self._end_time = val

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, val):
        self._offset = val

    def getKey(self):
        return self._start_time

    @classmethod
    def from_tuple(cls, start_time: datetime, sample: Any, end_time: datetime = None, offset: str = None):
        return cls(start_time=start_time, end_time=end_time, offset=offset, sample=sample)

    def __str__(self):
        return 'DataPoint(' + ', '.join(
            map(str, [self._start_time, self._end_time, self._offset, self._sample])) + ')\n'

    def __repr__(self):
        return 'DataPoint(' + ', '.join(
            map(str, [self._start_time, self._end_time, self._offset, self._sample])) + ')\n'

    def __lt__(self, dp):
        # if hasattr(dp, 'getKey'):
        return self.getKey().__lt__(dp.getKey())

    def __eq__(self, dp):
        return self._start_time == dp.start_time

    def __hash__(self):
        return hash(('start_time', self.start_time))

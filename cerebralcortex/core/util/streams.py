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

from typing import List
import uuid
from datetime import timedelta
from cerebralcortex.cerebralcortex import CerebralCortex


def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    :param dd_stream_id:
    """
    stream_days = []
    if stream_id:
        stream_duration = CC.get_stream_duration(stream_id)
        if stream_duration["start_time"] is not None and stream_duration["end_time"] is not None:
            days = stream_duration["end_time"] - stream_duration["start_time"]
            if days.seconds>60:
                total_days = days.days+2
            else:
                total_days = days.days+1
            for day in range(total_days):
                stream_days.append((stream_duration["start_time"] + timedelta(days=day)).strftime('%Y%m%d'))

    return stream_days

# Copyright (c) 2020, MD2K Center of Excellence
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

import os
from pennprov.connection.mprov_connection_cache import MProvConnectionCache
from pennprov.api.decorators import MProvAgg
from functools import wraps


def MProvAgg_empty():
    """
    This is an empty decorator. This will be applied if mprov server setting is OFF
    """
    def inner_function(func):
        @wraps(func)
        def wrapper(arg):
            return func(arg)
        return wrapper
    return inner_function


def CC_MProvAgg(in_stream_name,op,out_stream_name,in_stream_key=['index'],out_stream_key=['index'],map=None, graph_name=None):
    mprov = os.getenv("ENABLE_MPROV")
    if mprov=="True":
        connection_key = MProvConnectionCache.Key(user=os.getenv("MPROV_USER"), password=os.getenv("MPROV_PASSWORD"),
                                                  host=os.getenv("MPROV_HOST"), graph=graph_name)
        mprov_conn = MProvConnectionCache.get_connection(connection_key)
        if mprov_conn:
            mprov_conn.create_or_reset_graph()
        return MProvAgg(in_stream_name=in_stream_name, op=op, out_stream_name=out_stream_name, in_stream_key=in_stream_key, out_stream_key=out_stream_key, map=map, connection_key=connection_key)
    else:
        return MProvAgg_empty()
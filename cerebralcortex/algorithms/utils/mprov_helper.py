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

import inspect
import os
from datetime import timezone, datetime
from functools import wraps

from pennprov.api.decorators import MProvAgg
from pennprov.connection.mprov_connection_cache import MProvConnectionCache

from cerebralcortex.core.metadata_manager.stream.metadata import ModuleMetadata


def MProvAgg_empty():
    """
    This is an empty decorator. This will be applied if mprov server setting is OFF
    """
    def inner_function(func):
        @wraps(func)
        def wrapper(key, data):
            args_name = inspect.getfullargspec(func)[0]
            if len(args_name)>1:
                return func(key, data)
            else:
                return func(data)
        return wrapper
    return inner_function

def CC_get_prov_connection(graph_name=None):
    mprov = os.getenv("ENABLE_MPROV")
    if graph_name is None:
        graph_name = "mprov-graph"
    if mprov=="True":
        connection_key = MProvConnectionCache.Key(user=os.getenv("MPROV_USER"), password=os.getenv("MPROV_PASSWORD"),
                                                  host=os.getenv("MPROV_HOST"), graph=graph_name)
        mprov_conn = MProvConnectionCache.get_connection(connection_key)
        if mprov_conn:
            mprov_conn.get_low_level_api().create_provenance_graph(graph_name)
            return {"connection":mprov_conn, "connection_key":connection_key}
    return None

def CC_MProvAgg(in_stream_name,op,out_stream_name,in_stream_key=['index'],out_stream_key=['index'],map=None, graph_name=None):
    mprov_conn = CC_get_prov_connection(graph_name)
    if mprov_conn:
        return MProvAgg(in_stream_name=in_stream_name, op=op, out_stream_name=out_stream_name, in_stream_key=in_stream_key, out_stream_key=out_stream_key, connection_key=mprov_conn.get("connection_key"))
    else:
        return MProvAgg_empty()

def write_metadata_to_mprov(metadata):
    mprov = os.getenv("ENABLE_MPROV")
    if mprov=="True":
        mprov_connection = CC_get_prov_connection()
        if mprov_connection:
            mprov_conn = mprov_connection.get("connection")
            _prov_stream_node = None
            if mprov_conn is None:
                return

            # Initialize the stream node
            if _prov_stream_node is None:
                _prov_stream_node = mprov_conn.create_collection(metadata.name, 0)

            annot = {'description': metadata.description, 'annotations': str(metadata.annotations),
                     'schema': str(metadata.to_json().get("data_descriptor"))}

            mprov_conn.store_annotations(_prov_stream_node, annot)

            # Ensure there's a node for each input stream
            in_stream_nodes = []
            for in_stream in metadata.input_streams:
                in_stream_nodes.append(mprov_conn.create_collection(in_stream, 0))

            # Capture the derivation
            for in_stream in in_stream_nodes:
                mprov_conn.store_derived_from(_prov_stream_node, in_stream)

            now = datetime.now(timezone.utc)
            for module in metadata.modules:
                assert isinstance(module, ModuleMetadata)
                act = mprov_conn.store_activity(module.name, now, now, 0)
                for in_stream in in_stream_nodes:
                    mprov_conn.store_used(act, in_stream)
                    mprov_conn.store_generated_by(_prov_stream_node, act)
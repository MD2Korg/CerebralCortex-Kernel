# Copyright (c) 2017, MD2K Center of Excellence
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

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, ModuleMetadata


def update_metadata(stream_metadata, stream_name, stream_desc, module_name, module_version, authors:list, input_stream_names:list=[], annotations:list=[]) -> Metadata:
    """
    Create Metadata object with some sample metadata of phone battery data
    Args:
        stream_metadata:
        stream_name:
        stream_desc:
        module_name:
        module_version:
        authors (list[dict]): List of authors names and emails ids in dict. For example, authors = [{"ali":"ali@gmail.com"}, {"nasir":"nasir@gmail.com"}]
        input_stream_names:
        annotations:

    Returns:
        Metadata: metadata of phone battery stream
    """

    stream_metadata.set_name(stream_name).set_description(
        stream_desc) \
        .add_module(
        ModuleMetadata().set_name(
            module_name).set_version(
            module_version).set_authors(authors))\
        .add_annotation(annotations)\
        .add_input_stream(input_stream_names)

    stream_metadata.is_valid()
    return stream_metadata


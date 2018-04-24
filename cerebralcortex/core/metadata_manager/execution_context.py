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


class ExecutionContext:
    def __init__(self,
                 processing_module: dict = None,
                 input_parameters: dict = None,
                 input_streams: dict = None,
                 metadata: dict = None):
        """

        :param processing_module:
        :param input_parameters:
        :param input_streams:
        :param metadata:
        """
        self._processing_module = processing_module
        self._input_parameters = input_parameters
        self._input_streams = input_streams
        self._metadata = metadata

    def get_execution_context(self, processing_module: dict, algorithm: dict) -> dict:
        """
        Have a look at https://github.com/MD2Korg/CerebralCortex-DataAnalysis/tree/master/core/resources/metadata for schema and field details
        :param processing_module:
        :param algorithm:
        """
        ec = {**processing_module, **algorithm}
        execution_context = {"execution_context": ec}
        return execution_context

    def processing_module_schema(self, name, pm_description, input_params, input_streams):
        if not name:
            raise ValueError("Name is a mandatory field")
        elif not pm_description:
            raise ValueError("Processing module description is a mandatory field")
        elif not input_params:
            raise ValueError("Input params is mandatory field")
        else:
            processing_module = {
                "processing_module": {
                    "name": name,
                    "description": pm_description,
                    "input_parameters": input_params,
                    "input_streams": input_streams
                }
            }
        return processing_module

    def algorithm_schema(self, method, algo_description, authors, version, ref):

        if not method:
            raise ValueError("Complete path to the algorithm is mandatory field")
        elif not algo_description:
            raise ValueError("Algorithm description  is mandatory field")
        elif not authors:
            raise ValueError("Author(s) list  is mandatory field")
        elif not version:
            raise ValueError("Version is mandatory field")
        else:
            algo = {
                "algorithm": {
                    "method": method,
                    "description": algo_description,
                    "authors": authors,
                    "version": version,
                    "reference": ref
                }
            }

        return algo
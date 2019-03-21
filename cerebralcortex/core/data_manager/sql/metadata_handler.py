# Copyright (c) 2019, MD2K Center of Excellence
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


class MetadataHandler:

    def get_corrected_metadata(self, stream_name: str, status:str=["include"]) -> dict:
        """
        Retrieves corrected metadata

        Args:
            stream_name (str): name of a stream

        Returns:
            str: The value in the cache

        Raises:
            ValueError: if stream_name  is None/empty or error in executing query
        """
        if not stream_name:
            raise ValueError("stream_name cannot be empty.")
        
        
        qry = "select * from " + self.correctedMetadata + " where stream_name=%(stream_name)s group by stream_name"
        vals = {"stream_name": str(stream_name)}

        try:
            rows = self.execute(qry, vals)
            if len(rows) == 0:
                return {}
            else:
                metadata = rows[0]["metadata"].lower()
                return {"metadata":json.loads(metadata), "status":rows[0]["status"]}
        except Exception as e:
            raise Exception(str(e))

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


class DataIngestionHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def add_ingestion_log(self, user_id: str = "", stream_name: str = "", file_path: str = "", fault_type: str = "",
                          fault_description: str = "", success: int = None) -> bool:
        """
        Log errors and success of each record during data import process.

        Args:
            user_id (str): id of a user
            stream_name (str): name of a stream
            file_path (str): filename with its path
            fault_type (str): error type
            fault_description (str): error details
            success (int): 1 if data was successfully ingested, 0 otherwise

        Returns:
            bool

        Raises:
            ValeError: if
            Exception: if sql query fails user_id, file_path, fault_type, or success parameters is missing
        """

        if not user_id or not file_path or not fault_type or success is None:
            raise ValueError("user_id, file_path, fault_type, and success are mandatory parameters.")

        qry = "INSERT IGNORE INTO " + self.ingestionLogsTable + " (user_id, stream_name, file_path, fault_type, fault_description, success) VALUES(%s, %s, %s, %s, %s, %s)"
        vals = str(user_id), str(stream_name), str(file_path), str(fault_type), json.dumps(fault_description), success

        try:
            self.execute(qry, vals, commit=True)
            return True
        except Exception as e:
            raise Exception(e)

    def get_processed_files_list(self, success_type=False) -> list:
        """
        Get a list of all the processed files

        Returns:
            list: list of all processed files list
        """
        result = []
        if success_type:
            qry = "select file_path from " + self.ingestionLogsTable + " where success=%(success)s"
            vals = {"success":success_type}
            rows = self.execute(qry, vals)
        else:
            qry = "select file_path from " + self.ingestionLogsTable
            rows = self.execute(qry)

        if len(rows) == 0:
            return result
        else:
            for row in rows:
                result.append(row["file_path"])
            return result

    def get_ingestion_stats(self) -> list:
        """
        Get stats on ingested records


        Returns:
            dict: {"fault_type": str, "total_faults": int, "success":int}
        """
        result = []
        qry = "select fault_type, count(fault_type) as total_faults, success from " + self.ingestionLogsTable + " group by fault_type"

        rows = self.execute(qry)

        if len(rows) == 0:
            return result
        else:
            for row in rows:
                result.append({"fault_type": row["fault_type"], "total_faults": row["total_faults"]})
            return result

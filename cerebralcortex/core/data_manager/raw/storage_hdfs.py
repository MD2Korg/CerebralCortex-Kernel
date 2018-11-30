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

import gzip
import pickle
import traceback
import uuid
from datetime import datetime, timedelta
from typing import List

import pyarrow

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.util.data_types import deserialize_obj

class HDFSStorage():

    def __init__(self, obj):
        self.obj = obj

    def read_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                           end_time: datetime = None, localtime: bool = False) -> List[DataPoint]:
        """
        Read and Process (read, unzip, unpickle, remove duplicates) data from HDFS
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        # Using libhdfs,
        hdfs = pyarrow.hdfs.connect(self.obj.hdfs_ip, self.obj.hdfs_port)

        if localtime:
            days = [datetime.strftime(datetime.strptime(day, '%Y%m%d') - timedelta(hours=24), "%Y%m%d"), day,
                    datetime.strftime(datetime.strptime(day, '%Y%m%d') + timedelta(hours=24), "%Y%m%d")]
            day_start_time = datetime.strptime(day, '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:

                filename = self.obj.raw_files_dir + str(owner_id) + "/" + str(stream_id) + "/" + str(d) + ".pickle"
                gz_filename = filename.replace(".pickle", ".gz")
                data = None
                if hdfs.exists(filename):
                    curfile = hdfs.open(filename, "rb")
                    data = curfile.read()
                    #curfile.close()
                elif hdfs.exists(gz_filename):

                    curfile = hdfs.open(gz_filename, "rb")
                    data = curfile.read()
                    #curfile.close()
                    try:
                        data = gzip.decompress(data)
                        data = deserialize_obj(data)
                    except:
                        self.obj.logging.log(
                            error_message="Error! cannot decompress GZ file. FILE: "+gz_filename+" --- " + str(traceback.format_exc()),
                            error_type=self.obj.logtypes.CRITICAL)
                        #curfile.close()
                        #hdfs.delete(gz_filename)

                if data is not None and data != b'':
                    #clean_data = self.obj.filter_sort_datapoints(data)
                    self.obj.compress_store_pickle(filename, data, hdfs)
                    clean_data = self.obj.convert_to_localtime(data, localtime)
                    day_block.extend(self.obj.subset_data(clean_data, day_start_time, day_end_time))
            day_block = self.obj.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.obj.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            filename = self.obj.raw_files_dir + str(owner_id) + "/" + str(stream_id) + "/" + str(day) + ".pickle"
            gz_filename = filename.replace(".pickle", ".gz")
            data = None
            try:
                if hdfs.exists(filename):
                    curfile = hdfs.open(filename, "rb")
                    data = curfile.read()
                    #curfile.close()
                elif hdfs.exists(gz_filename):
                    curfile = hdfs.open(gz_filename, "rb")
                    data = curfile.read()
                    #curfile.close()
                    try:
                        data = gzip.decompress(data)
                        data = deserialize_obj(data)
                    except:
                        self.obj.logging.log(
                            error_message="Error! cannot decompress GZ file. FILE: "+gz_filename+" --- " + str(traceback.format_exc()),
                            error_type=self.obj.logtypes.CRITICAL)
                        #hdfs.delete(gz_filename)
                else:
                    return []
                if data is not None and data != b'':
                   # clean_data = self.obj.filter_sort_datapoints(data) TODO: Remove after testing. Already sorted and dedup during storage of data
                    self.obj.compress_store_pickle(filename, data, hdfs)
                    clean_data = self.obj.convert_to_localtime(data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.obj.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.obj.logging.log(
                    error_message="Error loading from HDFS: Cannot parse row. " + str(traceback.format_exc()),
                    error_type=self.obj.logtypes.CRITICAL)
                return []


    def write_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data in HDFS. If data contains multiple days then one file will be created for each day
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """

        # Using libhdfs
        hdfs = pyarrow.hdfs.connect(self.obj.hdfs_ip, self.obj.hdfs_port)
        outputdata = {}
        success = False

        # Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []

            outputdata[day].append(row)

        # Data Write loop
        for day, dps in outputdata.items():
            existing_data = None
            filename = self.obj.raw_files_dir + str(participant_id) + "/" + str(stream_id) + "/" + str(day) + ".gz"
            if len(dps) > 0:
                try:
                    if hdfs.exists(filename):
                        curfile = hdfs.open(filename, "rb")
                        existing_data = curfile.read()
                        #curfile.close()
                    if existing_data is not None and existing_data != b'':
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                        # dps = existing_data


                    dps = self.obj.filter_sort_datapoints(dps)
                    f = hdfs.open(filename, "wb")
                    dps = pickle.dumps(dps)
                    dps = gzip.compress(dps)
                    f.write(dps)
                    #f.close()

                    if hdfs.exists(filename.replace(".gz", ".pickle")):
                        hdfs.delete(filename.replace(".gz", ".pickle"))
                    success = True
                except Exception as ex:
                    success = False
                    # delete file if file was opened and no data was written to it
                    if hdfs.exists(filename):
                        if hdfs.info(filename)["size"] == 0:
                            hdfs.delete(filename)
                    self.obj.logging.log(
                        error_message="Error in writing data to HDFS. STREAM ID: " + str(
                            stream_id) + "Owner ID: " + str(participant_id) + "Files: " + str(
                            filename) + " - Exception: " + str(ex), error_type=self.obj.logtypes.DEBUG)
        return success

    # def get_file_contents(self, storage_type):
    #     if storage_type=="filesystem":
    #         return None
    #     elif storage_type=="hdfs":
    #         return None
    #     else:
    #         raise ValueError("Only filesystem or hdfs are supported file readers.")
    #
    # def file_exist(self, storage_type):
    #     if storage_type=="filesystem":
    #         return None
    #     elif storage_type=="hdfs":
    #         return None
    #     else:
    #         raise ValueError("Only filesystem or hdfs are supported file checkers.")

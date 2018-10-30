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
import os
import pickle
import traceback
import uuid
from datetime import datetime, timedelta
from typing import List

from cerebralcortex.core.datatypes.datapoint import DataPoint


class NoSQLStorage():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def read_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                                 end_time: datetime = None, localtime: bool = True) -> List[DataPoint]:
        """
        Read and Process (read, unzip, unpickle, remove duplicates) data from a file system
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        if localtime:
            days = [datetime.strftime(datetime.strptime(day, '%Y%m%d') - timedelta(hours=24), "%Y%m%d"), day,
                    datetime.strftime(datetime.strptime(day, '%Y%m%d') + timedelta(hours=24), "%Y%m%d")]
            day_start_time = datetime.strptime(day, '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:
                data = None
                filename = self.filesystem_path + str(owner_id) + "/" + str(stream_id) + "/" + str(d) + ".pickle"
                gz_filename = filename.replace(".pickle", ".gz")
                if os.path.exists(filename):
                    curfile = open(filename, "rb")
                    data = curfile.read()
                    curfile.close()
                elif os.path.exists(gz_filename):
                    curfile = open(gz_filename, "rb")
                    data = curfile.read()
                    curfile.close()
                    if data is not None and data != b'':
                        try:
                            data = gzip.decompress(data)
                        except:
                            self.logging.log(
                                error_message="Error! cannot decompress GZ file. FILE: "+gz_filename+" --- " + str(traceback.format_exc()),
                                error_type=self.logtypes.CRITICAL)
                            #os.remove(gz_filename)
                    
                if data is not None and data != b'':
                    #clean_data = self.filter_sort_datapoints(data) TODO: Remove after testing. Already sorted and dedup during storage of data
                    self.compress_store_pickle(filename, data)
                    clean_data = self.convert_to_localtime(data, localtime)
                    # day_start_time = datetime.fromtimestamp(day_start_time.timestamp(), clean_data[0].start_time.tzinfo)
                    # day_end_time = datetime.fromtimestamp(day_end_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_block.extend(self.subset_data(clean_data, day_start_time, day_end_time))

            day_block = self.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            filename = self.filesystem_path + str(owner_id) + "/" + str(stream_id) + "/" + str(day) + ".pickle"
            gz_filename = filename.replace(".pickle", ".gz")
            data = None

            try:
                if os.path.exists(filename):
                    curfile = open(filename, "rb")
                    data = curfile.read()
                    curfile.close()
                elif os.path.exists(gz_filename):
                    curfile = open(gz_filename, "rb")
                    data = curfile.read()
                    curfile.close()
                    try:
                        data = gzip.decompress(data)
                    except:
                        self.logging.log(
                            error_message="Error! cannot decompress GZ file. FILE: "+gz_filename+" --- " + str(traceback.format_exc()),
                            error_type=self.logtypes.CRITICAL)
                        #os.remove(gz_filename)
                    
                else:
                    return []
                if data is not None and data != b'':
                    #clean_data = self.filter_sort_datapoints(data) TODO: Remove after testing. Already sorted and dedup during storage of data
                    self.compress_store_pickle(filename, data)
                    clean_data = self.convert_to_localtime(data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.logging.log(
                    error_message="Error loading from FileSystem: Cannot parse row. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)
                return []

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def write_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data in file system. If data contains multiple days then one file will be created for each day
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """
        existing_data = None
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
            filename = self.filesystem_path + str(participant_id) + "/" + str(stream_id)
            if not os.path.exists(filename):
                os.makedirs(filename, exist_ok=True)
            filename = filename + "/" + str(day) + ".gz"
            if len(dps) > 0:
                try:
                    if os.path.exists(filename):
                        curfile = open(filename, "rb")
                        existing_data = curfile.read()
                        curfile.close()
                    if existing_data is not None and existing_data != b'':
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                        # dps = existing_data
                    dps = self.filter_sort_datapoints(dps)
                    f = open(filename, "wb")
                    dps = pickle.dumps(dps)
                    dps = gzip.compress(dps)
                    f.write(dps)
                    f.close()
                    if os.path.exists(filename.replace(".gz", ".pickle")):
                        os.remove(filename.replace(".gz", ".pickle"))
                    success = True
                except Exception as ex:
                    # delete file if file was opened and no data was written to it
                    if os.path.exists(filename):
                        if os.path.getsize(filename) == 0:
                            os.remove(filename)
                    self.logging.log(
                        error_message="Error in writing data to FileSystem. STREAM ID: " + str(
                            stream_id) + "Owner ID: " + str(participant_id) + "Files: " + str(
                            filename) + " - Exception: " + str(ex), error_type=self.logtypes.DEBUG)
        return success


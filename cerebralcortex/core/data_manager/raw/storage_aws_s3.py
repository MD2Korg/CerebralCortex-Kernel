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
from io import BytesIO
from typing import List

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.util.data_types import deserialize_obj

class AwsS3Storage():

    def __init__(self, obj):
        self.obj = obj

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def read_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                         end_time: datetime = None, localtime: bool = True) -> List[DataPoint]:
        """
        Read and Process (read, unzip, unpickle, remove duplicates) data from AWS-S3
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        # TODO: this method only works for .gz files. Update it to handle .pickle uncompressed if required

        bucket_name = self.obj.minio_input_bucket

        if localtime:
            days = [datetime.strftime(datetime.strptime(day, '%Y%m%d') - timedelta(hours=24), "%Y%m%d"), day,
                    datetime.strftime(datetime.strptime(day, '%Y%m%d') + timedelta(hours=24), "%Y%m%d")]
            day_start_time = datetime.strptime(day, '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:
                try:
                    object_name = self.obj.minio_dir_prefix+str(owner_id) + "/" + str(stream_id)+"/"+str(d)+".gz"

                    if self.obj.ObjectData.is_object(bucket_name, object_name):
                        data = self.obj.ObjectData.get_object(bucket_name, object_name)
                        if data.status==200:
                            data = gzip.decompress(data.data)
                            if data is not None and data != b'':
                                data = deserialize_obj(data)
                                #clean_data = self.obj.filter_sort_datapoints(data) TODO: Remove after testing. Already sorted and dedup during storage of data
                                clean_data = self.obj.convert_to_localtime(data, localtime)
                                day_block.extend(self.obj.subset_data(clean_data, day_start_time, day_end_time))
                        else:
                            self.obj.logging.log(
                                error_message="HTTP-STATUS: "+data.status+" - Cannot get "+str(object_name)+" from AWS-S3. " + str(traceback.format_exc()),
                                error_type=self.obj.logtypes.CRITICAL)
                except:
                    self.obj.logging.log(
                        error_message="Error loading from AWS-S3: " + str(traceback.format_exc()),
                        error_type=self.obj.logtypes.CRITICAL)

            #day_block = self.obj.filter_sort_datapoints(day_block) TODO: Remove after testing. Already sorted and dedup during storage of data
            if start_time is not None or end_time is not None:
                day_block = self.obj.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            object_name = self.obj.minio_dir_prefix+str(owner_id) + "/" + str(stream_id)+"/"+str(day)+".gz"
            try:
                if self.obj.ObjectData.is_object(bucket_name, object_name):
                    http_resp = self.obj.ObjectData.get_object(bucket_name, object_name)
                    if http_resp.status==200:
                        data = gzip.decompress(http_resp.data)
                    else:
                        self.obj.logging.log(
                            error_message=http_resp.status+" HTTP-STATUS, Cannot get data from AWS-S3. " + str(traceback.format_exc()),
                            error_type=self.obj.logtypes.CRITICAL)
                        return []
                else:
                    return []
                if data is not None and data != b'':
                    data = deserialize_obj(data)
                    #clean_data = self.obj.filter_sort_datapoints(data) TODO: Remove after testing. Already sorted and dedup during storage of data
                    clean_data = self.obj.convert_to_localtime(data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.obj.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.obj.logging.log(
                    error_message="Error loading from AWS-S3: Cannot parse row. " + str(traceback.format_exc()),
                    error_type=self.obj.logtypes.CRITICAL)
                return []
  
    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def write_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data in AWS-S3. If data contains multiple days then one file will be created for each day
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """

        bucket_name = self.obj.minio_output_bucket
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
            object_name = self.obj.minio_dir_prefix + str(participant_id) + "/" + str(stream_id) + "/" + str(day) + ".gz"

            if len(dps) > 0:
                try:
                    try:
                        if self.obj.ObjectData.is_object(bucket_name, object_name):
                            existing_data = self.obj.ObjectData.get_object(bucket_name, object_name)
                            existing_data = gzip.decompress(existing_data)
                            existing_data = pickle.loads(existing_data)
                            dps.extend(existing_data)
                    except:
                        pass
                    dps = self.obj.filter_sort_datapoints(dps)
                    dps = pickle.dumps(dps)
                    dps = gzip.compress(dps)
                    dps = BytesIO(dps)
                    obj_size = dps.seek(0, os.SEEK_END)
                    dps.seek(0) # set file pointer back to start, otherwise minio would complain as size 0
                    success = self.obj.ObjectData.upload_object_to_s3(bucket_name, object_name, dps, obj_size)
                except Exception as ex:
                    success = False
                    self.obj.logging.log(
                        error_message="Error in writing data to AWS-S3. STREAM ID: " + str(
                            stream_id) + "Owner ID: " + str(participant_id) + "Files: " + str(
                            object_name) + " - Exception: " + str(ex), error_type=self.obj.logtypes.DEBUG)
        return success

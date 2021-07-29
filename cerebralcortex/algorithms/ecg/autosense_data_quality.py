# Copyright (c) 2020, MD2K Center of Excellence
# All rights reserved.
# Md Azim Ullah (mullah@memphis.edu)
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

from pyspark.sql.functions import pandas_udf, PandasUDFType, lit
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, TimestampType, IntegerType

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata


def ecg_autosense_data_quality(ecg,
                               Fs=64,
                               sensor_name='autosense',
                               outlier_threshold_high = 4000,
                               outlier_threshold_low = 20,
                               slope_threshold = 100,
                               range_threshold=50,
                               eck_threshold_band_loose = 400,
                               window_size=3,
                               acceptable_outlier_percent = 34
                               ):
    """
    Some desc..

    Args:
        ecg (DataStream):
        Fs (int):
        sensor_name (str):
        outlier_threshold_high (int):
        outlier_threshold_low (int):
        slope_threshold (int):
        range_threshold (int):
        eck_threshold_band_loose (int):
        window_size (int):
        acceptable_outlier_percent (int):

    Returns:
        DataStream - structure [timestamp, localtime, version.....]
    """
    data_quality_band_loose = 'loose/improper attachment'
    data_quality_not_worn = 'sensor off body'
    data_quality_band_off = 'battery down/disconnected'
    data_quality_missing = 'intermittent data loss'
    data_quality_good = 'acceptable'
    stream_name = 'org.md2k.autosense.ecg.quality'

    def get_metadata():
        stream_metadata = Metadata()
        stream_metadata.set_name(stream_name).set_description("Chest ECG quality 3 seconds") \
            .add_input_stream(ecg.metadata.get_name()) \
            .add_dataDescriptor(DataDescriptor().set_name("timestamp").set_type("datetime")) \
            .add_dataDescriptor(DataDescriptor().set_name("localtime").set_type("datetime")) \
            .add_dataDescriptor(DataDescriptor().set_name("version").set_type("int")) \
            .add_dataDescriptor(DataDescriptor().set_name("user").set_type("string")) \
            .add_dataDescriptor(
            DataDescriptor().set_name("quality").set_type("string") \
                .set_attribute("description", "ECG data quality") \
                .set_attribute('Loose/Improper Attachment','Electrode Displacement') \
                .set_attribute('Sensor off Body', 'Autosense not worn') \
                .set_attribute('Battery down/Disconnected', 'No data is present - Can be due to battery down or sensor disconnection') \
                .set_attribute('Intermittent Data Loss','Not enough samples are present') \
                .set_attribute('Acceptable','Good Quality')) \
            .add_dataDescriptor(
            DataDescriptor().set_name("ecg").set_type("double").set_attribute("description", \
                                                                              "ecg sample value")) \
            .add_module(
            ModuleMetadata().set_name("ecg data quality").set_attribute("url", "http://md2k.org/").set_author(
                "Md Azim Ullah", "mullah@memphis.edu"))
        return stream_metadata

    def get_quality_autosense(data):
        """

        Args:
            data:

        Returns:

        """
        minimum_expected_samples = window_size*acceptable_outlier_percent*Fs/100

        if (len(data)== 0):
            return data_quality_band_off
        if (len(data)<=minimum_expected_samples) :
            return data_quality_missing
        range_data = max(data)-min(data)
        if range_data<=range_threshold:
            return data_quality_not_worn
        if range_data<=eck_threshold_band_loose:
            return data_quality_band_loose
        outlier_counts = 0
        for i in range(0,len(data)):
            im,ip  = i,i
            if i==0:
                im = len(data)-1
            else:
                im = i-1
            if i == len(data)-1:
                ip = 0
            else:
                ip = ip+1
            stuck = ((data[i]==data[im]) and (data[i]==data[ip]))
            flip = ((abs(data[i]-data[im])>((int(outlier_threshold_high)))) or (abs(data[i]-data[ip])>((int(outlier_threshold_high)))))
            disc = ((abs(data[i]-data[im])>((int(slope_threshold)))) and (abs(data[i]-data[ip])>((int(slope_threshold)))))
            if disc:
                outlier_counts += 1
            elif stuck:
                outlier_counts +=1
            elif flip:
                outlier_counts +=1
            elif data[i] >= outlier_threshold_high:
                outlier_counts +=1
            elif data[i]<= outlier_threshold_low:
                outlier_counts +=1
        if (100*outlier_counts>acceptable_outlier_percent*len(data)):
            return data_quality_band_loose
        return data_quality_good

    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("version", IntegerType()),
        StructField("user", StringType()),
        StructField("quality", StringType()),
        StructField("ecg", DoubleType())
    ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('ecg--org.md2k.autosense--autosense_chest--chest', 'ecg_autosense_data_quality', stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def data_quality(data):
        """

        Args:
            data:

        Returns:

        """
        data['quality'] = ''
        if data.shape[0]>0:
            data = data.sort_values('timestamp')
            if sensor_name in ['autosense']:
                data['quality'] = get_quality_autosense(list(data['ecg']))
        return data
    if sensor_name=='autosense':
        ecg_quality_stream = ecg.compute(data_quality,windowDuration=3,startTime='0 seconds')
    else:
        ecg_quality_stream = ecg.withColumn('quality',lit('acceptable'))
    data = ecg_quality_stream._data
    ds = DataStream(data=data,metadata=get_metadata())
    return ds





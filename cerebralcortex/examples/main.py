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

from pyspark.sql import functions as F
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.examples.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata

class Examples:
    def __init__(self, example_name):
        """
        load/set example params/data. This example perform following operations:
            - create sample phone battery data stream
            - perform windowing operation on the stream
            - store windowed data asa new stream
        Args:
            example_name:
        """

        self.setup_example()

        if example_name=="window":
            self.window_example()

    def setup_example(self):
        """
        setup required params for the example:
            - create cerebralcortex-kernel object
            - generate sample phone-battery data/metadata
            - create a DataStream object
            - save sample stream using cerebralcortex-kernel.
        """
        # create cerebralcortex object
        self.CC = CerebralCortex("../../conf/")

        # sample data params
        self.stream_name="BATTERY--org.md2k.phonesensor--PHONE"
        self.user_id = "00000000-afb8-476e-9872-6472b4e66b68"

        # generate sample phone-battery data/metadata
        data = gen_phone_battery_data(user_id=self.user_id)
        metadata = gen_phone_battery_metadata(stream_name=self.stream_name)

        # create a DataStream object
        ds = DataStream(data, metadata)

        # save sample data using cerebralcortex-kernal.
        # now we have some sample data stored in CerebralCortex format to play with!!!
        self.CC.save_stream(ds)

    def window_example(self):
        """
        This example will window phone battery stream into 1 minutes chunks and take the average of battery level

        """

        # get sample stream data
        ds = self.CC.get_stream(self.stream_name)
        data = ds.data # this is a pyspark dataframe object
        metadata = ds.metadata

        # print number of versions of the stream
        print("\n\n","*"*10, "STREAM VERSION", "*"*10)
        for md in metadata:
            print("stream-version:", md.version)

        # pyspark windowing operation
        # Note: do not include version column in the dataframe. Version is calculated and added by CerebralCortex-Kernel
        windowed_data = data.groupBy(['user',F.window("timestamp", "1 minute")]).agg(F.avg("battery_level").alias('battery_average'))
        windowed_data = windowed_data.select(windowed_data.user, windowed_data.window.start.alias("start"), windowed_data.window.end.alias("end"), windowed_data.battery_average)

        #TODO: I am performing operations on spark-dataframe. I do not know how to get all the required information to pass to mProv API.
        # Error: HTTP response body: {"message":"Missing endpoint: Endpoint {http://mprov.md2k.org}0 or {http://mprov.md2k.org}-_BATTERY--org.md2k.phonesensor--PHONEwBATTERY--org.md2k.phonesensor--PHONE.1 does not exist."}
        # discuss with Zack
        output_stream_index = 0 # TODO: not sure what it is
        input_tokens_list = [0,1,2] # TODO: not sure what it is
        self.CC.store_window_and_inputs(output_stream_name=self.stream_name,output_stream_index=output_stream_index, input_tokens_list=input_tokens_list)

        # print 5 samples from windowed data
        samples = windowed_data.take(5)
        print("\n\n","*"*10, "STREAM DATA", "*"*10)
        for sample in samples:
            print("User-ID:",sample.user,"Start-time:", sample.start, "End-time:", sample.end, "Average-battery-levels:", sample.battery_average)

        # save newly create data as a new stream in cerebralcortex
        new_stream_name = "BATTERY--org.md2k.phonesensor--PHONE-windowed-data"

        # create metadata for the new stream
        stream_metadata = Metadata()

        # Note: do not include version column in the dataframe. Version is calculated and added by CerebralCortex-Kernel
        stream_metadata.set_name(new_stream_name).add_description("1 minute windowed data of phone battery with average battery levels of each window.") \
            .add_dataDescriptor(
            DataDescriptor().name("start_time").type("datetime").set_attribute("description", "start time of a window")) \
            .add_dataDescriptor(
            DataDescriptor().name("end_time").type("datetime").set_attribute("description", "end time of a window")) \
            .add_dataDescriptor(
            DataDescriptor().name("battery_average").type("float").set_attribute("description", "average battery values of a window")) \
            .add_module(
            ModuleMetadata().name("cerebralcortex.examples.main").version("0.0.1").set_attribute("description", "CerebralCortex-kernel example code to window phone battery data").set_author(
                "test_user", "test_user@test_email.com"))

        # check whether metadata is valid and then store the datastream
        print("\n\n","*"*10, "STORING NEW STREAM DATA", "*"*10)
        if stream_metadata.is_valid():
            new_ds = DataStream(windowed_data, stream_metadata)
            if self.CC.save_stream(new_ds):
                print(new_stream_name, "has been stored.\n\n")


if __name__=="__main__":
    Examples("window")


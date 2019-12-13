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

from datetime import datetime
from pyspark.sql import functions as F
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.test_suite.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata


class DataStreamTest:

    def test_01_save_stream(self):
        """
        Test functionality related to save a stream

        """
        data = gen_phone_battery_data()
        metadata = gen_phone_battery_metadata()
        ds = DataStream(data, metadata)
        #dd = ds.filter_user("dfce1e65-2882-395b-a641-93f31748591b")
        result = self.CC.save_stream(ds)

        self.assertEqual(result, True)

    def test_02_stream(self):
        all_streams = self.CC.list_streams()
        searched_streams = self.CC.search_stream(stream_name="battery")

        self.assertEqual(len(all_streams),1)
        self.assertEqual(all_streams[0],self.stream_name)

        self.assertEqual(len(searched_streams),1)
        self.assertEqual(searched_streams[0],self.stream_name)

    def test_04_test_datafram_operations(self):
        ds = self.CC.get_stream(self.stream_name)
        avg_ds = ds.compute_average()
        data = avg_ds.collect()
        self.assertEqual(len(data.data),1)
        self.assertEqual(data.data[0][1],95.44044044044044)

        ds = self.CC.get_stream(self.stream_name)
        window_ds = ds.window()
        data = window_ds.collect()
        self.assertEqual(len(data.data),17)
        self.assertEqual(len(data.data[0][2]), 2)

        
    
    def test_05_map_window_to_stream(self):
        def get_val(lst):
            return lst[0]
        sum_vals_udf = F.udf(get_val)
        ds = self.CC.get_stream(self.stream_name)
        win_ds = ds.window()

        # convert window stream as quality stream for next step
        win_df=win_ds.data.withColumn("some_val", sum_vals_udf(win_ds.data["battery_level"])).drop("battery_level")

        from pyspark.sql.functions import pandas_udf, PandasUDFType
        import pandas as pd
        from pyspark.sql.types import StructField, StructType, StringType, FloatType
        schema = StructType([
            StructField("mean", FloatType()),
            StructField("val_1", FloatType()),
            StructField("val_2", FloatType())
        ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
        def mean_udf(v):
            all_cols = [[99,23,1.3]]
            df = pd.DataFrame(all_cols, columns=['mean', 'val_1', 'val_2'])
            return df
        win_ds=DataStream(data=win_ds.data.drop("window"), metadata=Metadata())
        new_ds = win_ds.compute(mean_udf)

        sd = new_ds.collect()
        df = win_df.withColumn("quality", F.when(win_df.some_val > 97, 1).otherwise(0)).drop("some_val")
        win_quality_ds = DataStream(data=df, metadata=Metadata())

        mapped_stream = ds.map_stream(win_quality_ds)
        filtered_stream = mapped_stream.filter("quality==0")
        bad_quality = filtered_stream.collect()
        self.assertEqual(len(bad_quality.data), 710)


    def test_03_get_stream(self):
        """
        Test functionality related to get a stream

        """
        ds = self.CC.get_stream(self.stream_name)
        df=ds.compute_stddev(60)
        df.show(5)
        data = ds
        
        metadata = ds.metadata[0]

        datapoint = data.take(1)

        self.assertEqual(datapoint[0][0], datetime(2019, 1, 9, 11, 35))
        self.assertEqual(datapoint[0][1], 100)
        self.assertEqual(datapoint[0][2], 1)
        self.assertEqual(datapoint[0][3], self.user_id)
        self.assertEqual(data.count(), 999)

        self.assertEqual(len(metadata.data_descriptor), 1)
        self.assertEqual(len(metadata.modules), 1)

        self.assertEqual(metadata.metadata_hash, self.metadata_hash)
        self.assertEqual(metadata.name, self.stream_name)
        self.assertEqual(metadata.version, int(self.stream_version))
        self.assertEqual(metadata.data_descriptor[0].name, 'battery_level')
        self.assertEqual(metadata.data_descriptor[0].type, 'longtype')
        self.assertEqual(metadata.data_descriptor[0].attributes.get("description"), 'current battery charge')
        self.assertEqual(metadata.modules[0].name, 'battery')
        self.assertEqual(metadata.modules[0].version, '1.2.4')
        self.assertEqual(metadata.modules[0].authors[0].get("test_user"), 'test_user@test_email.com')

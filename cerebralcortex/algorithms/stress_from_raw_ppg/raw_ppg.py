import pandas as pd
import os, sys
import numpy as np
import ast
import binascii

from cerebralcortex import Kernel
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.algorithms.stress_from_raw_ppg.model_wrappers import StressModel_RF
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType
from pyspark.sql.functions import pandas_udf, PandasUDFType

schema = StructType([
    StructField("user", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("stress_prediction", FloatType()),
    StructField("label", StringType()),
])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def process_raw_ppg(data: object) -> object:
    df = data.sort_values(by=['timestamp'])
    stress_model = StressModel_RF('/home/nndugudi/models/')
    predictions = []
    userid = df['user'].values[0]

    minute_chunk_list = []
    minute_chunk = []
    five_second_sliding_window = []

    ttmp = []
    ttmp.append(df.iloc[0]['timestamp'].timestamp())

    for b in df.iloc[0]['packet']:
      ttmp.append(np.int64(b) - 128)

    minute_chunk.append(np.array(ttmp).reshape(1,21))
    st = df['timestamp'].values[0]

    for x in range(1,len(df['timestamp'].values)):
      
      diff = (df.iloc[x]['timestamp'] - st) / np.timedelta64(1, 's')
      if diff <= 65.0:
        tmp = []
        tmp.append(df.iloc[x]['timestamp'].timestamp()) # convert seconds to ms
        #tmp.append(df.iloc[x]['packet'])
        for b in df.iloc[x]['packet']:
          tmp.append(np.int64(b) - 128)
        if len(tmp) != 21:
          continue
        minute_chunk.append(np.array(tmp).reshape(1,21))
        #if diff >= 60.0:
        #  five_second_sliding_window.append(df.iloc[x]['timestamp'])
      else:
        #minute_chunk_list.append(minute_chunk)
        nparr = np.concatenate(minute_chunk)
        pred = stress_model.predict(raw_data=nparr)
        if pred is not None:
          #print(pred['predictions'])
          if pred == 0 or pred == 1:
              predictions.append((userid, st, pred, 'GOOD'))
          if pred == -1:
              predictions.append((userid, st, None, 'INSUFFICIENT_DATA'))
          if pred == -2:
              predictions.append((userid, st, None, 'WATCH_IMPRORLY_WORN'))
          if pred == -3:
              predictions.append((userid, st, None, 'INSUFFICIENT_HEART_RATE'))
     
        #if len(five_second_sliding_window):
        #  minute_chunk = five_second_sliding_window
        #  five_second_sliding_window = []
        #else:
        #  minute_chunk = [minute_chunk[-1]]
        minute_chunk = []
        ttmp = []
        ttmp.append(df.iloc[x]['timestamp'].timestamp())
        for b in df.iloc[x]['packet']:
          ttmp.append(np.int64(b) - 128)
          #ttmp.append(int.from_bytes(b, 'little') - 128)
        minute_chunk.append(np.array(ttmp).reshape(1,21))
        st = df.iloc[x]['timestamp']
        # TODO

    df = pd.DataFrame(predictions, columns=['user', 'timestamp', 'stress_prediction', 'label'])
    return df

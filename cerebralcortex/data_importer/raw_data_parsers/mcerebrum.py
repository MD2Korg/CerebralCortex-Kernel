import json
import os
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from cerebralcortex import Kernel
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.data_manager.sql.data import SqlData


def assign_column_names_types(df, metadata=None):

    metadata_columns = []
    new_column_names ={0:"timestamp", 1:"localtime"}

    if metadata is not None:
        data_desciptor = metadata.get("data_descriptor", [])
        if isinstance(data_desciptor, dict):
            data_desciptor = [data_desciptor]

        for dd in data_desciptor:
            name = re.sub('[^a-zA-Z0-9]+', '_', dd.get("name", "", )).strip("_")
            metadata_columns.append({"name": name,"type": dd.get("data_type", "")})

    if len(metadata_columns)>0:
        col_no = 2 # first two column numbers are timestamp and offset
        for mc in metadata_columns:
            new_column_names[col_no] = mc["name"]
            col_no +=1
    else:
        for column in df:
            if column!=0 and column!=1:
                new_column_names[column] = "value_"+str(column-1)
    #                df[column] = pd.to_numeric(df[column], errors='ignore')

    df.rename(columns=new_column_names, inplace=True)
    for column in df:
        if column not in ['localtime','timestamp']:
            df[column] = pd.to_numeric(df[column], errors='ignore')
    return df

def mcerebrum_data_parser(line):
    data = []
    ts, offset, sample = line[0].split(',',2)
    try:
        ts = int(ts)
        offset = int(offset)
    except:
        raise Exception("cannot convert timestamp/offsets into int")
    try:
        vals = json.loads(sample)
    except:
        vals = sample.split(",")

    timestamp = datetime.utcfromtimestamp(ts/1000)
    localtime = datetime.utcfromtimestamp((ts+offset)/1000)
    data.append(timestamp)
    data.append(localtime)
    if isinstance(vals, list):

        data.extend(vals)
    else:
        data.append(vals)

    result = pd.Series(data)
    return result

# def parse_mcerebrum_data(data_file, metadata, parser=None):
#     df = pd.read_fwf(data_file, compression='gzip', header=None, quotechar='"')
#     df = df.apply(mcerebrum_data_parser, axis=1)
#     df = assign_column_names_types(df, metadata)
#     return df

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
from datetime import datetime
import pandas as pd
from cerebralcortex.data_importer.util.helper_methods import rename_column_name


def assign_column_names_types(df: pd, metadata: dict = None) -> pd:
    """
    Change column names to the names defined in metadata->data_descriptor block

    Args:
        df (pandas): pandas dataframe
        metadata (dict): metadata of the data

    Returns:
        pandas dataframe
    """
    metadata_columns = []
    new_column_names = {0: "timestamp", 1: "localtime"}

    if metadata is not None:
        data_desciptor = metadata.get("data_descriptor", [])
        if isinstance(data_desciptor, dict):
            data_desciptor = [data_desciptor]

        for dd in data_desciptor:
            name = rename_column_name(dd.get("name", "", ))
            metadata_columns.append({"name": name, "type": dd.get("data_type", "")})

    if len(metadata_columns) > 0:
        col_no = 2  # first two column numbers are timestamp and offset
        for mc in metadata_columns:
            new_column_names[col_no] = mc["name"]
            col_no += 1
    else:
        for column in df:
            if column != 0 and column != 1:
                new_column_names[column] = "value_" + str(column - 1)

    df.rename(columns=new_column_names, inplace=True)
    for column in df:
        if column not in ['localtime', 'timestamp']:
            df[column] = pd.to_numeric(df[column], errors='ignore')
    return df


def mcerebrum_data_parser(line: str) -> list:
    """
    parse each row of data file into list of values (timestamp, localtime, val1, val2....)

    Args:
        line (str):

    Returns:
        list: (timestamp, localtime, val1, val2....)
    """
    data = []
    ts, offset, sample = line[0].split(',', 2)
    try:
        ts = int(ts)
        offset = int(offset)
    except:
        raise Exception("cannot convert timestamp/offsets into int")
    try:
        vals = json.loads(sample)
    except:
        vals = sample.split(",")

    timestamp = datetime.utcfromtimestamp(ts / 1000)
    localtime = datetime.utcfromtimestamp((ts + offset) / 1000)
    data.append(timestamp)
    data.append(localtime)
    if isinstance(vals, list):
        data.extend(vals)
    else:
        data.append(vals)

    return data

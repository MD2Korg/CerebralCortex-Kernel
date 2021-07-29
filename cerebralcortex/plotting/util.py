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

import pandas as pd


def ds_to_pdf(ds, user_id=None) -> pd.DataFrame:
    """
    converts DataStream object into pandas dataframe
    Args:
        ds (DataStream):

    Returns:
        pandas.DataFrame
    """


    if isinstance(ds, pd.DataFrame):
        if user_id:
            pdf = ds.loc[ds['user'] == user_id]
        else:
            pdf = ds
    else:
        if user_id:
            ds = ds.filter_user(user_id)
        pdf = ds._data.toPandas()

    if "timestamp" in pdf.columns:
        return pdf.sort_values('timestamp')
    return pdf

def _remove_cols(pdf:pd.DataFrame, cols:list=["user", "version", "timestamp", "localtimestamp", "localtime", "window"]):
    """
    remove DataFrame columns

    Args:
        pdf (pd.DataFrame):
        cols (list):

    Returns:

    """
    for col in cols:
        if col in pdf.columns:
            del pdf[col]
    return pdf
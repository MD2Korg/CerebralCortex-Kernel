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

import cufflinks as cf
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import iplot, init_notebook_mode
from cerebralcortex.plotting.util import ds_to_pdf, _remove_cols
from cerebralcortex.core.datatypes.datastream import DataStream

def plot_timeseries(ds: DataStream, user_id:str, y_axis_column:str=None):
    """
    line plot of timeseries data

    Args:
        ds (DataStream):
        user_id (str): uuid of a user
        y_axis_column (str): x axis column is hard coded as timestamp column. only y-axis can be passed as a param
    """
    pdf = ds_to_pdf(ds, user_id)
    cf.set_config_file(offline=True, world_readable=True, theme='ggplot')
    init_notebook_mode(connected=True)
    ts = pdf['timestamp']
    pdf = _remove_cols(pdf)
    if y_axis_column:
        data = [go.Scatter(x=ts, y=pdf[str(y_axis_column)])]
        iplot(data, filename = 'time-series-plot')
    else:
        iplot([{
            'x': ts,
            'y': pdf[col],
            'name': col
        }  for col in pdf.columns], filename='time-series-plot')

def plot_hist(ds, user_id:str, x_axis_column=None):
    """
    histogram plot of timeseries data

    Args:
        ds (DataStream):
        user_id (str): uuid of a user
        x_axis_column (str): x axis column of the plot
    """

    pdf = ds_to_pdf(ds, user_id)
    cf.set_config_file(offline=True, world_readable=True, theme='ggplot')
    init_notebook_mode(connected=True)
    pdf = _remove_cols(pdf)
    if x_axis_column:
        data = [go.Histogram(x=pdf[str(x_axis_column)])]
        iplot(data, filename='basic histogram')
    else:
        pdf.iplot(kind='histogram', filename='basic histogram')

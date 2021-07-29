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


import plotly.graph_objs as go
from plotly.subplots import make_subplots
from cerebralcortex.plotting.util import ds_to_pdf


def plot_timeseries(ds, user_id:str=None, x_axis_column:str="timestamp", y_axis_column:list="all", graph_title:str="Graph"):
    """
    line plot of timeseries data

    Args:
        ds:
        user_id (str): uuid of a user
        x_axis_column (str): timestamp or localtime are acceptable values only
        y_axis_column (list): set this to "all" if you want to plot all columns
        graph_title (str): title of the graph
    """
    pdf = ds_to_pdf(ds, user_id)

    user_ids = list(pdf.user.unique())
    subplot_titles = ["Participant ID: {}".format(x.upper()) for x in list(pdf.user.unique())]

    if x_axis_column not in ["timestamp", "localtime"]:
        raise Exception(" X axis can only be timestamp or localtime column.")

    if y_axis_column=="all":
        y_axis = sorted(list(set(pdf.columns.to_list())-set(["timestamp","user","version", "localtime"])))
    elif isinstance(y_axis_column, list):
        y_axis = y_axis_column
    elif isinstance(y_axis_column, str):
        y_axis = [y_axis_column]

    fig = make_subplots(rows=len(user_ids), cols=1, subplot_titles=subplot_titles)
    row_id = 1
    for sid in user_ids:
        if user_ids[-1]==sid:
            lagend = True
        else:
            lagend = False
        for y in y_axis:
            fig.add_trace(go.Scatter(
                x=pdf[x_axis_column][pdf.user==sid],
                y=pdf[y][pdf.user==sid],
                showlegend=lagend,
                name=y
            ), row=row_id, col=1)
        fig.update_xaxes(title_text="Timestamp", row=row_id, col=1)

        row_id +=1
    height = 500*len(user_ids)

    fig.update_layout(height=height, width=900, title_text=graph_title)
    fig.show()


def plot_box(ds, user_id:str=None, x_axis_column:str="timestamp", y_axis_column:list="all", graph_title:str="Graph"):
    """
    Box plot of timeseries data

    Args:
        ds: CC DataStream object or Pandas DataFrame object
        user_id (str): uuid of a user
        x_axis_column (str): timestamp or localtime are acceptable values only
        y_axis_column (list): set this to "all" if you want to plot all columns
        graph_title (str): title of the graph
    """
    pdf = ds_to_pdf(ds, user_id)

    user_ids = list(pdf.user.unique())
    subplot_titles = ["Participant ID: {}".format(x.upper()) for x in list(pdf.user.unique())]

    if x_axis_column not in ["timestamp", "localtime"]:
        raise Exception(" X axis can only be timestamp or localtime column.")

    if y_axis_column=="all":
        y_axis = sorted(list(set(pdf.columns.to_list())-set(["timestamp","user","version", "localtime"])))
    elif isinstance(y_axis_column, list):
        y_axis = y_axis_column
    elif isinstance(y_axis_column, str):
        y_axis = [y_axis_column]

    fig = make_subplots(rows=len(user_ids), cols=1, subplot_titles=subplot_titles)
    row_id = 1
    for sid in user_ids:
        if user_ids[-1]==sid:
            lagend = True
        else:
            lagend = False
        for y in y_axis:
            fig.add_trace(go.Box(
                y=pdf[y][pdf.user==sid],
                showlegend=lagend,
                name=y
            ), row=row_id, col=1)
        fig.update_xaxes(row=row_id, col=1)

        row_id +=1
    height = 500*len(user_ids)

    fig.update_layout(height=height, width=900, title_text=graph_title)
    fig.show()


def plot_histogram(ds, user_id:str=None, x_axis_column:str="timestamp", y_axis_column:list="all", graph_title:str="Graph"):
    """
    Histogram plot of timeseries data

    Args:
        ds: CC DataStream object or Pandas DataFrame object
        user_id (str): uuid of a user
        x_axis_column (str): timestamp or localtime are acceptable values only
        y_axis_column (list): set this to "all" if you want to plot all columns
        graph_title (str): title of the graph
    """
    pdf = ds_to_pdf(ds, user_id)

    user_ids = list(pdf.user.unique())
    subplot_titles = ["Participant ID: {}".format(x.upper()) for x in list(pdf.user.unique())]

    if x_axis_column not in ["timestamp", "localtime"]:
        raise Exception(" X axis can only be timestamp or localtime column.")

    if y_axis_column=="all":
        y_axis = sorted(list(set(pdf.columns.to_list())-set(["timestamp","user","version", "localtime"])))
    elif isinstance(y_axis_column, list):
        y_axis = y_axis_column
    elif isinstance(y_axis_column, str):
        y_axis = [y_axis_column]

    fig = make_subplots(rows=len(user_ids), cols=1, subplot_titles=subplot_titles)
    row_id = 1
    for sid in user_ids:
        if user_ids[-1]==sid:
            lagend = True
        else:
            lagend = False
        for y in y_axis:
            fig.add_trace(go.Histogram(
                x=pdf[y][pdf.user==sid],
                showlegend=lagend,
                name=y
            ), row=row_id, col=1)
        fig.update_xaxes(row=row_id, col=1)

        row_id +=1
    height = 500*len(user_ids)

    fig.update_layout(height=height, width=900,
                      title_text=graph_title,
                      xaxis_title_text='Value',
                      yaxis_title_text='Count',
                      bargap=0.2,
                      bargroupgap=0.1
                      )
    fig.show()

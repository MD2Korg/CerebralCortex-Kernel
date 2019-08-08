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

import plotly.plotly as py
import plotly.graph_objs as go
import pandas as pd
import random
from datetime import datetime, timedelta
import cufflinks as cf
from plotly.offline import iplot, init_notebook_mode
import pandas as pd
from datetime import datetime
from ipyleaflet import Map, Marker, MarkerCluster


class BasicPlots():
    def remove_cols(self, pdf, cols=["user", "version", "timestamp", "localtimestamp", "localtime", "window"]):
        for col in cols:
            if col in pdf.columns:
                del pdf[col]
        return pdf

    def timeseries(self, pdf, y_axis_column=None):
        cf.set_config_file(offline=True, world_readable=True, theme='ggplot')
        init_notebook_mode(connected=True)
        ts = pdf['timestamp']
        pdf = self.remove_cols(pdf)
        if y_axis_column:
            data = [go.Scatter(x=ts, y=pdf[str(y_axis_column)])]
            iplot(data, filename = 'time-series-plot')
        else:
            iplot([{
                'x': ts,
                'y': pdf[col],
                'name': col
            }  for col in pdf.columns], filename='time-series-plot')

    def hist(self, pdf, x_axis_column=None):
        cf.set_config_file(offline=True, world_readable=True, theme='ggplot')
        init_notebook_mode(connected=True)
        pdf = self.remove_cols(pdf)
        if x_axis_column:
            data = [go.Histogram(x=pdf[str(x_axis_column)])]
            iplot(data, filename='basic histogram')
        else:
            pdf.iplot(kind='histogram', filename='basic histogram')

    def plot_gps_cords(self, pdf, zoom=5):
        marker_list = []
        center = None
        for index, row in pdf.iterrows():
            if center is None:
                center = [row["latitude"], row["longitude"]]
            marker_list.append(Marker(location=(row["latitude"], row["longitude"])))

        m = Map(center=(center), zoom=zoom)
        marker_cluster = MarkerCluster(
            markers=(marker_list)
        )
        m.add_layer(marker_cluster);
        return m


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
import plotly.figure_factory as ff


class StressStreamPlots():
    def plot_pie(self, pdf, group_by_column=None):
        pdf=pdf.groupby(str(group_by_column), as_index=False).agg('count')
        labels=[]
        values=[]
        for index, row in pdf.iterrows():
            labels.append(row["stresser_main"])
            values.append(row["density"])

        trace = go.Pie(labels=labels, values=values)
        iplot([trace], filename='stresser_pie_chart')

    def plot_gantt(self, pdf):
        data=[]
        for index, row in pdf.iterrows():
            data.append(dict(Task=row["stresser_sub"], Start=row["start_time"], Finish=row["end_time"], Resource=row["stresser_main"]))

            fig = ff.create_gantt(data, index_col='Resource', title='Stressers, Main & Sub Categories',
                                  show_colorbar=True, bar_width=0.8, showgrid_x=True, showgrid_y=True)
            fig['layout']['yaxis'].update({"showticklabels":False})
            iplot(fig, filename='gantt-hours-minutes')
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

    def plot_sankey(self, df,cat_cols=[], value_cols='',title="Stressers' Sankey Diagram"):
        labelList = []

        for catCol in cat_cols:
            labelListTemp =  list(set(df[catCol].values))
            labelList = labelList + labelListTemp

        # remove duplicates from labelList
        labelList = list(dict.fromkeys(labelList))

        # transform df into a source-target pair
        for i in range(len(cat_cols)-1):
            if i==0:
                sourceTargetDf = df[[cat_cols[i],cat_cols[i+1],value_cols]]
                sourceTargetDf.columns = ['source','target','density']
            else:
                tempDf = df[[cat_cols[i],cat_cols[i+1],value_cols]]
                tempDf.columns = ['source','target','density']
                sourceTargetDf = pd.concat([sourceTargetDf,tempDf])
            sourceTargetDf = sourceTargetDf.groupby(['source','target']).agg({'density':'sum'}).reset_index()

        # add index for source-target pair
        sourceTargetDf['sourceID'] = sourceTargetDf['source'].apply(lambda x: labelList.index(x))
        sourceTargetDf['targetID'] = sourceTargetDf['target'].apply(lambda x: labelList.index(x))

        # creating the sankey diagram
        data = dict(
            type='sankey',
            node = dict(
                pad = 15,
                thickness = 20,
                line = dict(
                    color = "black",
                    width = 0.5
                ),
                label = labelList
            ),
            link = dict(
                source = sourceTargetDf['sourceID'],
                target = sourceTargetDf['targetID'],
                value = sourceTargetDf['density']
            )
        )

        layout =  dict(
            title = title,
            font = dict(
                size = 10
            )
        )

        fig = dict(data=[data], layout=layout)
        iplot(fig, validate=False)

    def plot_bar(self, pdf, x_axis_column=None):
        grouped_pdf=pdf.groupby(["user_id",x_axis_column], as_index=False).agg('sum')
        user_ids = pdf.groupby("user_id", as_index=False).last()

        data = []
        x_col_names = []
        # get the x-axis labels that has max length
        for index, row in user_ids.iterrows():
            sub=grouped_pdf.loc[grouped_pdf['user_id'] == row["user_id"]]
            if len(x_col_names)<len(sub[x_axis_column]):
                x_col_names = sub[x_axis_column]

        for index, row in user_ids.iterrows():
            sub=grouped_pdf.loc[grouped_pdf['user_id'] == row["user_id"]]
            sub.sort_values(x_axis_column)

            data.append(go.Bar({
                'y': sub["density"],
                'x': sorted(x_col_names),
                'name': row["user_id"]
            }))

        iplot(data, filename='basic-line')

    def plot_comparison(self, pdf, x_axis_column=None, usr_id=None, compare_with="all"):
        data = []
        if usr_id:
            usr_data = pdf.loc[pdf['user_id'] == str(usr_id)]

            if compare_with =="all" or compare_with is None:
                compare_with_data = pdf.loc[pdf['user_id'] != str(usr_id)]
            else:
                compare_with_data = pdf.loc[pdf['user_id'] == str(compare_with)]

            grouped_user_pdf=usr_data.groupby([x_axis_column], as_index=False).agg('sum')
            grouped_compare_with_pdf=compare_with_data.groupby([x_axis_column], as_index=False).agg('sum')

            x_col_names = []
            # get the x-axis labels that has max length
            tmp = grouped_user_pdf.append(grouped_compare_with_pdf, ignore_index=True)

            x_col_names = list(set(tmp[x_axis_column]))

            data.append(go.Bar({
                'y': grouped_user_pdf["density"],
                'x': x_col_names,
                'name': usr_id
            }))
            if compare_with=="all":
                compare_with = "All Users"
            data.append(go.Bar({
                'y': grouped_compare_with_pdf["density"],
                'x': x_col_names,
                'name': compare_with
            }))

            iplot(data, filename='basic-line')

        else:
            raise Exception("usr_id cannot be None/Blank.")


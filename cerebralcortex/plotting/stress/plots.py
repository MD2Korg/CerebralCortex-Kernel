# Copyright (c) 2020, MD2K Center of Excellence
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

def plot_stress_pie(ds, x_axis_column="stresser_main"):
    pdf = ds._data.toPandas()
    pdf = ds._sort_values(pdf)
    ds._stress_plots.plot_pie(pdf, x_axis_column)


def plot_stress_gantt(ds):
    pdf = ds._data.toPandas()
    pdf = ds._sort_values(pdf)
    ds._stress_plots.plot_gantt(pdf)


def plot_stress_sankey(ds, cat_cols=["stresser_main", "stresser_sub"], value_cols='density',
                       title="Stressers' Sankey Diagram"):
    pdf = ds._data.toPandas()
    pdf = ds._sort_values(pdf)
    ds._stress_plots.plot_sankey(df=pdf, cat_cols=cat_cols, value_cols=value_cols, title=title)


def plot_stress_bar(ds, x_axis_column="stresser_main"):
    pdf = ds._data.toPandas()
    pdf = ds._sort_values(pdf)
    ds._stress_plots.plot_bar(pdf, x_axis_column=x_axis_column)


def plot_stress_comparison(ds, x_axis_column="stresser_main", usr_id=None, compare_with="all"):
    pdf = ds._data.toPandas()
    pdf = ds._sort_values(pdf)
    ds._stress_plots.plot_comparison(pdf, x_axis_column=x_axis_column, usr_id=usr_id, compare_with=compare_with)
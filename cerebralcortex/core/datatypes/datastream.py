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

from pyspark.sql import functions as F
from pyspark.sql.functions import udf, collect_list
from typing import List
from pyspark.sql.types import *
#from pyspark.sql.functions import pandas_udf,PandasUDFType
from pyspark.sql.window import Window
from cerebralcortex.core.plotting.basic_plots import BasicPlots
from cerebralcortex.core.plotting.stress_plots import StressStreamPlots

import re
import sys

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


class DataStream:
    def __init__(self,
                 data: object = None,
                 metadata: Metadata = None
                 ):
        """
        DataStream object contains pyspark dataframe and metadata linked to it.

        Args:
            data (DataFrame): pyspark dataframe
            metadata (Metadata): metadata of data

        """

        self._data = data
        self._metadata = metadata
        self._basic_plots = BasicPlots()
        self._stress_plots = StressStreamPlots()

    def get_metadata(self, version:int=None)->Metadata:
        """
        get stream metadata

        Args:
            version (int): version of a stream

        Returns:
            Metadata: single version of a stream
        Raises:
            Exception: if specified version is not available for the stream

        """
        for md in self._metadata:
            if md.version == version:
                return md
            else:
                raise Exception("Version '"+str(version)+"' is not available for this stream.")
        return None

    @property
    def metadata(self):
        """
        return stream metadata

        Returns:
            Metadata:

        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata):
        """
        set stream metadata

        Args:
            metadata (Metadata):
        """
        self._metadata = metadata

    @property
    def data(self):
        """
        get stream data

        Returns (DataFrame):

        """
        return self._data

    @data.setter
    def data(self, value):
        """
        set stream data

        Args:
            value (DataFrame):
        """
        self._data = value

    #############################################################################
    #                           Helper methods for dataframe                    #
    #############################################################################

    # !!!!                                  HELPER METHODS                           !!!
    def to_pandas(self):
        """
        This method converts pyspark dataframe into pandas dataframe.

        Notes:
            This method will collect all the data on master node to convert pyspark dataframe into pandas dataframe.
            After converting to pandas dataframe datastream objects helper methods will not be accessible.
            
        Returns:
            Datastream (Metadata, pandas.DataFrame): this will return a new datastream object with blank metadata 
            
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = CC.get_stream("STREAM-NAME")
            >>> new_ds = ds.to_pandas()
            >>> new_ds.data.head()
        """
        pdf = self._data.toPandas()
        if "timestamp" in pdf.columns:
            pdf = pdf.sort_values('timestamp')
        return DataStream(data=pdf, metadata=Metadata())


    def collect(self):
        """
        Collect all the data to master node and return list of rows

        Returns:
            List: rows of all the dataframe
        """
        return DataStream(data=self._data.collect(), metadata=Metadata())


    # !!!!                                  STAT METHODS                           !!!

    def compute_average(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute average of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): average will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="avg", columnName=colmnName)

    def compute_sqrt(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute square root of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): square root will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="sqrt", columnName=colmnName)

    def compute_sum(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute sum of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): average will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="sum", columnName=colmnName)

    def compute_variance(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute variance of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): variance will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="variance", columnName=colmnName)

    def compute_stddev(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute standard deviation of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): standard deviation will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="stddev", columnName=colmnName)

    def compute_min(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute min of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): min value will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="min", columnName=colmnName)

    def compute_max(self, windowDuration:int=None, colmnName:str=None)->object:
        """
        Window data and compute max of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            colmnName (str): max  will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="max", columnName=colmnName)


    def _compute_stats(self, windowDuration:int=None, methodName:str=None, columnName:List[str]=[])->object:
        """
        Compute stats on pyspark dataframe

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            methodName (str): pyspark stat method name
            columnName (str): max  will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        exprs = self._get_column_names(columnName=columnName, methodName=methodName)
        if windowDuration:
            windowDuration = str(windowDuration)+" seconds"
            win = F.window("timestamp", windowDuration)
            result = self._data.groupBy(['user',win]).agg(exprs)
        else:
            result = self._data.groupBy(['user']).agg(exprs)

        result = self._update_column_names(result)
        return DataStream(data=result, metadata=Metadata())


    # !!!!                              WINDOWING METHODS                           !!!

    def window(self, windowDuration:int=60, groupByColumnName:List[str]=[], columnName:List[str]=[], slideDuration:int=None, startTime=None, preserve_ts=False):
        """
        Window data into fixed length chunks. If no columnName is provided then the windowing will be performed on all the columns.

        Args:
            windowDuration (int): duration of a window in seconds
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            columnName List[str]: column names on which windowing should be performed. Windowing will be performed on all columns if none is provided
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            preserve_ts (bool): setting this to True will return timestamps of corresponding to each windowed value
        Returns:
            DataStream: this will return a new datastream object with blank metadata
        Note:
            This windowing method will use collect_list to return values for each window. collect_list is not optimized.

        """
        windowDuration = str(windowDuration)+" seconds"
        if slideDuration is not None:
            slideDuration = str(slideDuration)+" seconds"
        exprs = self._get_column_names(columnName=columnName, methodName="collect_list", preserve_ts=preserve_ts)
        win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)
        if len(groupByColumnName)>0:
            groupByColumnName.append("user")
            groupByColumnName.append("version")
            groupByColumnName.append(win)
            windowed_data = self._data.groupBy(groupByColumnName).agg(exprs)
        else:
            windowed_data = self._data.groupBy(['user','version',win]).agg(exprs)

        data = windowed_data

        data = self._update_column_names(data)

        return DataStream(data=data, metadata=Metadata())

    def _update_column_names(self, data):
        columns = []
        for column in data.columns:
            if "(" in column:
                m = re.search('\((.*?)\)', column)
                columns.append(m.group(1))
            else:
                columns.append(column)
        return data.toDF(*columns)

    # !!!!                              FILTERING METHODS                           !!!

    def drop_column(self, *args, **kwargs):
        """
        calls deafult dataframe drop

        Args:
            *args:
            **kwargs:
        """
        data = self._data.drop(*args, **kwargs)
        return DataStream(data=data, metadata=Metadata())

    def summary(self):
        """
        print the summary of the data
        """
        self._data.describe().show(truncate=False)

    def limit(self, *args, **kwargs):
        """
        calls deafult dataframe limit

        Args:
            *args:
            **kwargs:
        """
        data = self._data.limit(*args, **kwargs)
        return DataStream(data=data, metadata=Metadata())

    def where(self, *args, **kwargs):
        """
        calls deafult dataframe where

        Args:
            *args:
            **kwargs:
        """
        data = self._data.where(*args, **kwargs)
        return DataStream(data=data, metadata=Metadata())

    def filter(self, columnName, operator, value):
        """
        filter data

        Args:
            columnName (str): name of the column
            operator (str): basic operators (e.g., >, <, ==, !=)
            value (Any): if the columnName is timestamp, please provide python datatime object

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        where_clause = columnName+operator+"'"+str(value)+"'"
        data = self._data.where(where_clause)
        return DataStream(data=data, metadata=Metadata())

    def map_stream(self, window_ds):
        """
        Map/join a stream to a windowed stream

        Args:
            window_ds (Datastream): windowed datastream object

        Returns:
            Datastream: joined/mapped stream

        """
        window_ds = window_ds.data.drop("version", "user")
        df= window_ds.join(self.data, self.data.timestamp.between(F.col("window.start"), F.col("window.end")))
        return DataStream(data=df, metadata=Metadata())

    def filter_user(self, user_ids:List):
        """
        filter data to get only selective users' data

        Args:
            user_ids (List[str]): list of users' UUIDs
        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        if not isinstance(user_ids, list):
            user_ids = [user_ids]
        data = self._data.where(self._data["user"].isin(user_ids))
        return DataStream(data=data, metadata=Metadata())

    def filter_version(self, version:List):
        """
        filter data to get only selective users' data

        Args:
            version (List[str]): list of stream versions

        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Todo:
            Metadata version should be return with the data
        """
        if not isinstance(version, list):
            version = [version]
        data = self._data.where(self._data["version"].isin(version))
        return DataStream(data=data, metadata=Metadata())

    def groupby(self, *columnName):
        """
        Group data by column name
        Args:
            columnName (str): name of the column to group by with

        Returns:

        """
        data = self._data.groupby(*columnName)
        return DataStream(data=data, metadata=Metadata())

    # def win(self, udfName):
    #     self._data = self._data.groupBy(['owner', F.window("timestamp", "60 seconds")]).apply(udfName)
    #     self.metadata = Metadata()
    #     return self

    def compute(self, udfName, timeInterval=None):
        if 'custom_window' in self._data.columns:
            data = self._data.groupby('user','custom_window').apply(udfName)
        else:
            data = self._data.groupby('user').apply(udfName)
        return DataStream(data=data, metadata=Metadata())

    def run_algorithm(self, udfName, columnNames:List[str]=[], windowDuration:int=60, slideDuration:int=None, groupByColumnName:List[str]=[], startTime=None, preserve_ts=False):
        """
        Run an algorithm

        Args:
            udfName: Name of the algorithm
            columnName List[str]: column names on which windowing should be performed. Windowing will be performed on all columns if none is provided
            windowDuration (int): duration of a window in seconds
            slideDuration (int): slide duration of a window
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            preserve_ts (bool): setting this to True will return timestamps of corresponding to each windowed value
        Returns:
            DataStream: this will return a new datastream object with blank metadata

        """
        windowDuration = str(windowDuration)+" seconds"
        groupbycols = ["user", "version"]

        win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)

        if len(groupByColumnName)>0:
            groupbycols.extend(groupByColumnName)

        groupbycols.append(win)

        if len(columnNames)==0:
            raise ValueError("columnNames list cannot be empty.")

        tmp = ""
        for col in columnNames:
            tmp += "collect_list({}{}{}){}".format('"',col,'"',",")

        tmp = "{}{}{}{}".format("udfName", "(", tmp.rstrip(","), ")")
        merged_column = self._data.groupBy(groupbycols).agg(eval(tmp).alias("merged_column"))

        cols = merged_column.schema.fields
        new_cols = ["timestamp"]
        for col in cols:
            if col.name=="merged_column":
                for cl in col.dataType.names:
                    new_cols.append("merged_column."+cl)
            else:
                new_cols.append(col.name)

        merged_column=merged_column.withColumn("timestamp", merged_column.window.start)

        data = merged_column.select(new_cols)

        return DataStream(data=data, metadata=Metadata())

    def sort(self, columnNames:list=[], ascending=True):
        """
        Sort data column in ASC or DESC order

        Returns:
            object: DataStream object
        """
        ascending_list = []
        if len(columnNames)==0:
            columnNames.append("timestamp")

        for col in columnNames:
            if ascending:
                ascending_list.append(1)
            else:
                ascending_list.append(0)
        data = self._data.orderBy(columnNames,ascending=ascending_list)
        return DataStream(data=data, metadata=Metadata())

    # def run_algo(self, udfName, windowSize:str="1 minute"):
    #     """
    #
    #     Args:
    #         udfName: Name of the algorithm
    #         windowSize: acceptable_params are "1 second", "1 minute", "1 hour" OR "1 day"
    #     """
    #     acceptable_params = ["1 second", "1 minute", "1 hour", "1 day"]
    #
    #     if windowSize=="1 second":
    #         extended_df = self._data.withColumn("groupby_col",F.concat(F.col("timestamp").cast("date"), F.lit("-"), F.hour(F.col("timestamp")), F.lit("-"), F.minute(F.col("timestamp")), F.lit("-"), F.second(F.col("timestamp"))).cast("string"))
    #     elif windowSize=="1 minute":
    #         extended_df = self._data.withColumn("groupby_col",F.concat(F.col("timestamp").cast("date"), F.lit("-"), F.hour(F.col("timestamp")), F.lit("-"), F.minute(F.col("timestamp"))).cast("string"))
    #     elif windowSize=="1 hour":
    #         extended_df = self._data.withColumn("groupby_col",F.concat(F.col("timestamp").cast("date"), F.lit("-"), F.hour(F.col("timestamp"))).cast("string"))
    #     elif windowSize=="1 day":
    #         extended_df = self._data.withColumn("groupby_col",F.concat(F.col("timestamp").cast("date")).cast("string"))
    #     else:
    #         raise ValueError(str(windowSize)+" is not an acceptable param. Acceptable params are only: "+" OR ".join(acceptable_params))
    #
    #     data = extended_df.groupBy("user","version","groupby_col").apply(udfName)
    #     return DataStream(data=data, metadata=Metadata())

    def show(self, *args, **kwargs):
        self._data.show(*args, **kwargs)

    def schema(self):
        """
        Get data schema (e.g., column names and number of columns etc.)

        Returns:
            pyspark dataframe schema object
        """
        return self._data.schema

    def _get_column_names(self, columnName:List[str], methodName:str, preserve_ts:bool=False):
        """
        Get data column names and build expression for pyspark aggregate method

        Args:
            columnName(List[str]): get all column names expression if columnName is empty
            methodName (str): name of the method that should be applied on the column
        Todo:
            update non-data column names
        Returns:
            dict: {columnName: methodName}
        """
        columns = self._data.columns
        black_list_column = ["timestamp", "localtime", "user", "version"]

        if "localtime" not in columns:
            black_list_column.pop(1)
        elif preserve_ts:
            black_list_column.pop(1)

        if preserve_ts:
            black_list_column.pop(0)

        if columnName:
            if isinstance(columns, str):
                columns = [columnName]
            elif isinstance(columns, list):
                columns = columnName
        else:
            columns = list(set(columns)-set(black_list_column))

        exprs = {x: methodName for x in columns}
        return exprs


############################### PLOTS ###############################
    def _sort_values(self, pdf):
        if "timestamp" in pdf.columns:
            return pdf.sort_values('timestamp')
        return pdf

    def plot(self, y_axis_column=None):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._basic_plots.timeseries(pdf, y_axis_column=y_axis_column)
        
    def plot_hist(self, x_axis_column=None):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._basic_plots.hist(pdf, x_axis_column=x_axis_column)

    def plot_gps_cords(self, zoom=5):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        return self._basic_plots.plot_gps_cords(pdf, zoom=zoom)

    def plot_stress_pie(self, x_axis_column="stresser_main"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_pie(pdf, x_axis_column)

    def plot_stress_gantt(self):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_gantt(pdf)

    def plot_stress_sankey(self, cat_cols=["stresser_main","stresser_sub"], value_cols='density',title="Stressers' Sankey Diagram"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_sankey(df=pdf,cat_cols=cat_cols, value_cols=value_cols, title=title)

    def plot_stress_bar(self, x_axis_column="stresser_main"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_bar(pdf, x_axis_column=x_axis_column)

    def plot_stress_comparison(self, x_axis_column="stresser_main", usr_id=None, compare_with="all"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_comparison(pdf, x_axis_column=x_axis_column, usr_id=usr_id, compare_with=compare_with)



###################### New Methods by Anand #########################


    def join(self, dataStream, propagation='forward'):
        """
        filter data

        Args:
            columnName (str): name of the column
            operator (str): basic operators (e.g., >, <, ==, !=)
            value (Any): if the columnName is timestamp, please provide python datatime object

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        combined_df = self._data.join(dataStream.data, on = ['user','timestamp', 'localtime','version'], how='full').orderBy('timestamp')
        combined_filled = combined_df.withColumn("data_quality", F.last('data_quality', True).over(Window.partitionBy('user').orderBy('timestamp').rowsBetween(-sys.maxsize, 0)))
        combined_filled_filtered = combined_filled.filter(combined_filled.ecg.isNotNull())

        return DataStream(data=combined_filled_filtered, metadata=Metadata())



    def create_windows(self, window_length='hour'):
        """
        filter data

        Args:
            columnName (str): name of the column
            operator (str): basic operators (e.g., >, <, ==, !=)
            value (Any): if the columnName is timestamp, please provide python datatime object

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        windowed_df = self._data.withColumn('custom_window', windowing_udf('timestamp'))
        return DataStream(data=windowed_df, metadata=Metadata())

from pyspark.sql.functions import hour, mean
"""
Windowing function to customize the parallelization of computation.
"""
def get_window(x):
    u = '_'
    y = x.year
    m = x.month
    d = x.day
    h = x.hour
    mi = x.minute
    s = str(y) + u + str(m) + u + str(d) + u + str(h) #+ u + str(mi)

    return s

windowing_udf = udf(get_window, StringType())



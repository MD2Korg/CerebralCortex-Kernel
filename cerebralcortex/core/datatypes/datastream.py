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
from typing import List
import re

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


        if isinstance(metadata, Metadata):
            if metadata.is_valid():
                self._data = data
                self._metadata = metadata
            else:
                raise Exception("Metadata is not valid.")
        else:
            self._data = data
            self._metadata = metadata

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
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = CC.get_stream("STREAM-NAME")
            >>> pandas_ds = ds.to_pandas()
            >>> pandas_ds.data.head()
        """
        self._data = self._data.toPandas()

    def collect(self):
        """
        Collect all the data to master node and return list of rows

        Returns:
            List: rows of all the dataframe
        """
        return self._data.collect()


    # !!!!                                  STAT METHODS                           !!!

    def compute_average(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute average of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): average will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="avg", columnName=colmnName)

    def compute_sqrt(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute square root of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): square root will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="sqrt", columnName=colmnName)

    def compute_sum(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute sum of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): average will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="sum", columnName=colmnName)

    def compute_variancee(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute variance of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): variance will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="variance", columnName=colmnName)

    def compute_stddev(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute standard deviation of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): standard deviation will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="stddev", columnName=colmnName)

    def compute_min(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute min of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): min value will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="min", columnName=colmnName)

    def compute_max(self, windowDuration:int=60, colmnName:str=None)->object:
        """
        Window data and compute max of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds
            colmnName (str): max  will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, methodName="max", columnName=colmnName)


    def _compute_stats(self, windowDuration:int=60, methodName:str=None, columnName:List[str]=[])->object:
        """
        Compute stats on pyspark dataframe

        Args:
            windowDuration (int): duration of a window in seconds
            methodName (str): pyspark stat method name
            columnName (str): max  will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """

        windowDuration = str(windowDuration)+" seconds"
        exprs = self._get_column_names(columnName=columnName, methodName=methodName)
        result = self._data.groupBy(['user',F.window("timestamp", windowDuration)]).agg(exprs)

        self._data = result
        self._update_column_names()
        self.metadata = Metadata()
        return self


    # !!!!                              WINDOWING METHODS                           !!!

    def window(self, windowDuration:int=60, groupByColumnName:List[str]=[], columnName:List[str]=[], slideDuration:int=None, startTime=None):
        """
        Window data into fixed length chunks. If no columnName is provided then the windowing will be performed on all the columns.

        Args:
            windowDuration (int): duration of a window in seconds
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            columnName List[str]: column names on which windowing should be performed. Windowing will be performed on all columns if none is provided
            slideDuration (int): slide duration of a window
            startTime (datetime): start time of window. First time of data will be used as startTime if none is provided
        Returns:
            DataStream: this will return a new datastream object with blank metadata
        Note:
            This windowing method will use collect_list to return values for each window. collect_list is not optimized.

        """
        windowDuration = str(windowDuration)+" seconds"


        exprs = self._get_column_names(columnName=columnName, methodName="collect_list")
        if len(groupByColumnName)>0:
            windowed_data = self._data.groupBy(['user','timestamp', F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)]).agg(exprs)
        else:
            windowed_data = self._data.groupBy(['user',F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)]).agg(exprs)

        self._data = windowed_data

        self._update_column_names()

        self.metadata = Metadata()

        return self

    def _update_column_names(self):
        columns = []
        for column in self._data.columns:
            if "(" in column:
                m = re.search('\((.*?)\)', column)
                columns.append(m.group(1))
            else:
                columns.append(column)
        self._data = self._data.toDF(*columns)

    # !!!!                              FILTERING METHODS                           !!!

    def drop_column(self, *args, **kwargs):
        """
        calls deafult dataframe drop

        Args:
            *args:
            **kwargs:
        """
        self._data = self._data.drop(*args, **kwargs)

    def limit(self, *args, **kwargs):
        """
        calls deafult dataframe limit

        Args:
            *args:
            **kwargs:
        """
        self._data = self._data.limit(*args, **kwargs)

    def where(self, *args, **kwargs):
        """
        calls deafult dataframe where

        Args:
            *args:
            **kwargs:
        """
        self._data = self._data.where(*args, **kwargs)

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
        result = self._data.where(where_clause)
        self._data = result
        self.metadata = Metadata()
        return self

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
        result = self._data.where(self._data["user"].isin(user_ids))
        self._data = result
        self.metadata = Metadata()
        return self

    def filter_version(self, version:List):
        """
        filter data to get only selective users' data

        Args:
            version (List[str]): list of stream versions
        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        if not isinstance(version, list):
            version = [version]
        result = self._data.where(self._data["version"].isin(version))
        self._data = result
        self.metadata = Metadata()
        return self

    def groupby(self, columnName):
        """
        Group data by column name
        Args:
            columnName (str): name of the column to group by with

        Returns:

        """
        self._data = self._data.groupby(columnName)
        self.metadata = Metadata()
        return self

    # def win(self, udfName):
    #     self._data = self._data.groupBy(['owner', F.window("timestamp", "60 seconds")]).apply(udfName)
    #     self.metadata = Metadata()
    #     return self

    def marker(self, udfName):
        self._data = self._data.apply(udfName)
        self.metadata = Metadata()
        return self

    def show(self, *args, **kwargs):
        self._data.show(*args, **kwargs)

    def schema(self):
        """
        Get data schema (e.g., column names and number of columns etc.)

        Returns:
            pyspark dataframe schema object
        """
        return self._data.schema

    def _get_column_names(self, columnName:List[str], methodName:str):
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

        if "localtime" in columns:
            black_list_column = ["timestamp", "localtime", "user", "version"]
        else:
            black_list_column = ["timestamp", "user", "version"]

        if columnName:
            if isinstance(columns, str):
                columns = [columnName]
            elif isinstance(columns, list):
                columns = columnName
        else:
            columns = list(set(columns)-set(black_list_column))

        exprs = {x: methodName for x in columns}
        return exprs

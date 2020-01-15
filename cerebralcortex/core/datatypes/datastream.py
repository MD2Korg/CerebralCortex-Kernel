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

import re
import sys
from typing import List
import math
import pandas as pd
import numpy as np

from datetime import timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
# from pyspark.sql.functions import pandas_udf,PandasUDFType
from operator import attrgetter
from pyspark.sql.types import StructType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.window import Window

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.plotting.basic_plots import BasicPlots
from cerebralcortex.core.plotting.stress_plots import StressStreamPlots


class DataStream(DataFrame):
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

        if isinstance(data, DataFrame):
            super(self.__class__, self).__init__(data._jdf, data.sql_ctx)

        if self._metadata is not None and not isinstance(self.metadata,list) and len(self.metadata.data_descriptor)==0 and data is not None:
            self.metadata = self._gen_metadata()

    # !!!!                       Disable some of dataframe operations                           !!!
    def write(self):
        raise NotImplementedError

    def writeStream(self):
        raise NotImplementedError

    def get_metadata(self, version: int = None) -> Metadata:
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
                raise Exception("Version '" + str(version) + "' is not available for this stream.")
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
        # raise Exception("Cannot access data. Please use DataStream object to perform all the operations.")
        return self._data

    @data.setter
    def data(self, value):
        """
        set stream data

        Args:
            value (DataFrame):
        """
        self._data = value

    # !!!!                                  STAT METHODS                           !!!

    def compute_average(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute average of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): average will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="avg", columnName=colmnName)

    def compute_sqrt(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute square root of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): square root will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="sqrt", columnName=colmnName)

    def compute_sum(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute sum of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): average will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="sum", columnName=colmnName)

    def compute_variance(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute variance of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): variance will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="variance", columnName=colmnName)

    def compute_stddev(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute standard deviation of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): standard deviation will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="stddev", columnName=colmnName)

    def compute_min(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute min of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): min value will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="min", columnName=colmnName)

    def compute_max(self, windowDuration: int = None, slideDuration:int=None, startTime=None, colmnName: str = None) -> object:
        """
        Window data and compute max of a windowed data of a single or all columns

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            colmnName (str): max  will be computed for all the columns if columnName param is not provided (for all windows)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        return self._compute_stats(windowDuration=windowDuration, slideDuration=slideDuration,startTime=startTime, methodName="max", columnName=colmnName)

    def _compute_stats(self, windowDuration: int = None, slideDuration:int=None, startTime=None, methodName: str = None, columnName: List[str] = []) -> object:
        """
        Compute stats on pyspark dataframe

        Args:
            windowDuration (int): duration of a window in seconds. If it is not set then stats will be computed for the whole data in a column(s)
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            methodName (str): pyspark stat method name
            columnName (str): max  will be computed for all the columns if columnName param is not provided (for all windows)
            
            
        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """       
        exprs = self._get_column_names(columnName=columnName, methodName=methodName)
        if windowDuration:
            windowDuration = str(windowDuration) + " seconds"
            win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)
            result = self._data.groupBy(['user','version', win]).agg(exprs)
        else:
            result = self._data.groupBy(['user','version']).agg(exprs)


        result = result.withColumn("timestamp",result.window.start)
        # to get local time - agg/window won't return localtimestamp col
        offset = (self._data.first().timestamp - self._data.first().localtime).total_seconds()
        result = result.withColumn("localtime", result.window.start+F.expr("INTERVAL "+str(offset)+" SECONDS"))
        result = self._update_column_names(result)
        return DataStream(data=result, metadata=Metadata())

    # !!!!                              WINDOWING METHODS                           !!!

    def window(self, windowDuration: int = 60, groupByColumnName: List[str] = [], columnName: List[str] = [],
               slideDuration: int = None, startTime=None, preserve_ts=False):
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
        windowDuration = str(windowDuration) + " seconds"
        if slideDuration is not None:
            slideDuration = str(slideDuration) + " seconds"
        exprs = self._get_column_names(columnName=columnName, methodName="collect_list", preserve_ts=preserve_ts)
        win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)
        if len(groupByColumnName) > 0:
            groupByColumnName.append("user")
            groupByColumnName.append("version")
            groupByColumnName.append(win)
            windowed_data = self._data.groupBy(groupByColumnName).agg(exprs)
        else:
            windowed_data = self._data.groupBy(['user', 'version', win]).agg(exprs)

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

    def map_stream(self, window_ds):
        """
        Map/join a stream to a windowed stream

        Args:
            window_ds (Datastream): windowed datastream object

        Returns:
            Datastream: joined/mapped stream

        """
        window_ds = window_ds.data.drop("version", "user")
        df = window_ds.join(self.data, self.data.timestamp.between(F.col("window.start"), F.col("window.end")))
        return DataStream(data=df, metadata=Metadata())

    def filter_user(self, user_ids: List):
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

    def filter_version(self, version: List):
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

    def compute_magnitude(self, col_names=[], magnitude_col_name="magnitude"):
        if len(col_names)<1:
            raise Exception("col_names param is missing.")
        tmp = ""
        for col_name in col_names:
            tmp += 'F.col("'+col_name+'")*F.col("'+col_name+'")+'
        tmp = tmp.rstrip("+")

        data = self._data.withColumn(magnitude_col_name, F.sqrt(eval(tmp)))
        return DataStream(data=data, metadata=Metadata())

    def interpolate(self, freq=16, method='linear', axis=0, limit=None, inplace=False,
                    limit_direction='forward', limit_area=None,
                    downcast=None):
        """
        Interpolate values according to different methods. This method internally uses pandas interpolation.

        Args:
            freq (int): Frequency of the signal
            method (str): default ‘linear’
                - ‘linear’: Ignore the index and treat the values as equally spaced. This is the only method supported on MultiIndexes.
                - ‘time’: Works on daily and higher resolution data to interpolate given length of interval.
                - ‘index’, ‘values’: use the actual numerical values of the index.
                - ‘pad’: Fill in NaNs using existing values.
                - ‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘spline’, ‘barycentric’, ‘polynomial’: Passed to scipy.interpolate.interp1d. These methods use the numerical values of the index. Both ‘polynomial’ and ‘spline’ require that you also specify an order (int), e.g. df.interpolate(method='polynomial', order=5).
                - ‘krogh’, ‘piecewise_polynomial’, ‘spline’, ‘pchip’, ‘akima’: Wrappers around the SciPy interpolation methods of similar names. See Notes.
                - ‘from_derivatives’: Refers to scipy.interpolate.BPoly.from_derivatives which replaces ‘piecewise_polynomial’ interpolation method in scipy 0.18.
            axis  {0 or ‘index’, 1 or ‘columns’, None}: default None. Axis to interpolate along.
            limit (int): optional. Maximum number of consecutive NaNs to fill. Must be greater than 0.
            inplace (bool): default False. Update the data in place if possible.
            limit_direction {‘forward’, ‘backward’, ‘both’}: default ‘forward’. If limit is specified, consecutive NaNs will be filled in this direction.
            limit_area  {None, ‘inside’, ‘outside’}: default None. If limit is specified, consecutive NaNs will be filled with this restriction.
                - None: No fill restriction.
                - ‘inside’: Only fill NaNs surrounded by valid values (interpolate).
                - ‘outside’: Only fill NaNs outside valid values (extrapolate).
            downcast optional, ‘infer’ or None: defaults to None
            **kwargs: Keyword arguments to pass on to the interpolating function.

        Returns DataStream: interpolated data

        """
        schema = self._data.schema
        sample_freq = 1000/freq
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def interpolate_data(pdf):
            pdf.set_index("timestamp", inplace=True)
            pdf = pdf.resample(str(sample_freq)+"ms").bfill(limit=1).interpolate(method=method, axis=axis, limit=limit,inplace=inplace, limit_direction=limit_direction, limit_area=limit_area, downcast=downcast)
            pdf.ffill(inplace=True)
            pdf.reset_index(drop=False, inplace=True)
            pdf.sort_index(axis=1, inplace=True)
            return pdf

        data = self._data.groupby(["user", "version"]).apply(interpolate_data)
        return DataStream(data=data,metadata=Metadata())

    def complementary_filter(self, freq:int=16, accelerometer_x:str="accelerometer_x",accelerometer_y:str="accelerometer_y",accelerometer_z:str="accelerometer_z", gyroscope_x:str="gyroscope_x", gyroscope_y:str="gyroscope_y", gyroscope_z:str="gyroscope_z"):
        """
        Compute complementary filter on gyro and accel data.
        Args:
            freq (int): frequency of accel/gryo. Assumption is that frequency is equal for both gyro and accel. 
            accelerometer_x (str): name of the column 
            accelerometer_y (str): name of the column
            accelerometer_z (str): name of the column
            gyroscope_x (str): name of the column
            gyroscope_y (str): name of the column
            gyroscope_z (str): name of the column
        """
        dt = 1.0 / freq  # 1/16.0;
        M_PI = math.pi;
        hpf = 0.90;
        lpf = 0.10;

        window = Window.partitionBy(self._data['user']).orderBy(self._data['timestamp'])

        data = self._data.withColumn("thetaX_accel",
                             ((F.atan2(-F.col(accelerometer_z), F.col(accelerometer_y)) * 180 / M_PI)) * lpf) \
            .withColumn("roll",
                        (F.lag("thetaX_accel").over(window) + F.col(gyroscope_x) * dt) * hpf + F.col("thetaX_accel")).drop("thetaX_accel") \
            .withColumn("thetaY_accel",
                             ((F.atan2(-F.col(accelerometer_x), F.col(accelerometer_z)) * 180 / M_PI)) * lpf) \
            .withColumn("pitch",
                        (F.lag("thetaY_accel").over(window) + F.col(gyroscope_y) * dt) * hpf + F.col("thetaY_accel")).drop("thetaY_accel")\
            .withColumn("thetaZ_accel",
                             ((F.atan2(-F.col(accelerometer_y), F.col(accelerometer_x)) * 180 / M_PI)) * lpf) \
            .withColumn("yaw",
                        (F.lag("thetaZ_accel").over(window) + F.col(gyroscope_z) * dt) * hpf + F.col("thetaZ_accel")).drop("thetaZ_accel")

        return DataStream(data=data.dropna(),metadata=Metadata())


    def compute(self, udfName, windowDuration: int = None, slideDuration: int = None,
                      groupByColumnName: List[str] = [], startTime=None):
        """
        Run an algorithm. This method supports running an udf method on windowed data

        Args:
            udfName: Name of the algorithm
            windowDuration (int): duration of a window in seconds
            slideDuration (int): slide duration of a window
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
        Returns:
            DataStream: this will return a new datastream object with blank metadata

        """
        if slideDuration:
            slideDuration = str(slideDuration) + " seconds"
            
        if 'custom_window' in self._data.columns:
            data = self._data.groupby('user', 'custom_window').apply(udfName)
        else:
            groupbycols = ["user", "version"]
        
            if windowDuration:
                windowDuration = str(windowDuration) + " seconds"
                win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)
                groupbycols.append(win)

            if len(groupByColumnName) > 0:
                groupbycols.extend(groupByColumnName)



            data = self._data.groupBy(groupbycols).apply(udfName)

        return DataStream(data=data, metadata=Metadata())

    # def compute(self, udfName, timeInterval=None):
    #     if 'custom_window' in self._data.columns:
    #         data = self._data.groupby('user', 'custom_window').apply(udfName)
    #     else:
    #         data = self._data.groupby('user','version').apply(udfName)
    #     return DataStream(data=data, metadata=Metadata())

    # def run_algorithm(self, udfName, columnNames: List[str] = [], windowDuration: int = 60, slideDuration: int = None,
    #                   groupByColumnName: List[str] = [], startTime=None):
    #     """
    #     Run an algorithm. This method supports running an udf method on windowed data
    #
    #     Args:
    #         udfName: Name of the algorithm
    #         columnName List[str]: column names on which windowing should be performed. Windowing will be performed on all columns if none is provided
    #         windowDuration (int): duration of a window in seconds
    #         slideDuration (int): slide duration of a window
    #         groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
    #         startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
    #     Returns:
    #         DataStream: this will return a new datastream object with blank metadata
    #
    #     """
    #     windowDuration = str(windowDuration) + " seconds"
    #     groupbycols = ["user", "version"]
    #
    #     win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)
    #
    #     if len(groupByColumnName) > 0:
    #         groupbycols.extend(groupByColumnName)
    #
    #     groupbycols.append(win)
    #
    #     # if len(columnNames) == 0:
    #     #     raise ValueError("columnNames list cannot be empty.")
    #
    #     # tmp = ""
    #     # for col in columnNames:
    #     #     tmp += "collect_list({}{}{}){}".format('"', col, '"', ",")
    #
    #     # tmp = "{}{}{}{}".format("udfName", "(", tmp.rstrip(","), ")")
    #     merged_column = self._data.groupBy(groupbycols).apply(udfName)
    #
    #     # cols = merged_column.schema.fields
    #     # new_cols = ["timestamp"]
    #     # for col in cols:
    #     #     if col.name == "merged_column":
    #     #         for cl in col.dataType.names:
    #     #             new_cols.append("merged_column." + cl)
    #     #     else:
    #     #         new_cols.append(col.name)
    #     #
    #     # merged_column = merged_column.withColumn("timestamp", merged_column.window.start)
    #     #
    #     # data = merged_column.select(new_cols)
    #
    #     return DataStream(data=merged_column, metadata=Metadata())

    def _get_column_names(self, columnName: List[str], methodName: str, preserve_ts: bool = False):
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
            columns = list(set(columns) - set(black_list_column))

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

    def plot_stress_sankey(self, cat_cols=["stresser_main", "stresser_sub"], value_cols='density',
                           title="Stressers' Sankey Diagram"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_sankey(df=pdf, cat_cols=cat_cols, value_cols=value_cols, title=title)

    def plot_stress_bar(self, x_axis_column="stresser_main"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_bar(pdf, x_axis_column=x_axis_column)

    def plot_stress_comparison(self, x_axis_column="stresser_main", usr_id=None, compare_with="all"):
        pdf = self._data.toPandas()
        pdf = self._sort_values(pdf)
        self._stress_plots.plot_comparison(pdf, x_axis_column=x_axis_column, usr_id=usr_id, compare_with=compare_with)

    #############################################################################
    #                           Wrapper for PySpark Methods                     #
    #############################################################################
    def alias(self, alias):
        """
        Returns a new DataStream with an alias set.

        Args:
            alias: string, an alias name to be set for the datastream.

        Returns:
            object: DataStream object

        Examples:
            >>> df_as1 = df.alias("df_as1")
            >>> df_as2 = df.alias("df_as2")
        """
        data = self._data.alias(alias)
        return DataStream(data=data, metadata=Metadata())

    def agg(self, *exprs):
        """
        Aggregate on the entire DataStream without groups

        Args:
            *exprs:

        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Examples:
            >>> ds.agg({"age": "max"}).collect()
            >>> # Below example shows how to use pyspark functions in add method
            >>> from pyspark.sql import functions as F
            >>> ds.agg(F.min(ds.age)).collect()
        """
        data = self._data.agg(*exprs)
        return DataStream(data=data, metadata=Metadata())

    def approxQuantile(self,col, probabilities, relativeError):
        """
        Calculates the approximate quantiles of numerical columns of a DataStream.

        The result of this algorithm has the following deterministic bound: If the DataStream has N elements and if we request the quantile at probability p up to error err, then the algorithm will return a sample x from the DataStream so that the exact rank of x is close to (p * N). More precisely,

        floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

        This method implements a variation of the Greenwald-Khanna algorithm (with some speed optimizations). The algorithm was first present in [[http://dx.doi.org/10.1145/375663.375670 Space-efficient Online Computation of Quantile Summaries]] by Greenwald and Khanna.

        Note that null values will be ignored in numerical columns before calculation. For columns only containing null values, an empty list is returned.

        Args:
            col (str[list]): Can be a single column name, or a list of names for multiple columns.

            probabilities: a list of quantile probabilities Each number must belong to [0, 1]. For example 0 is the minimum, 0.5 is the median, 1 is the maximum.

            relativeError: The relative target precision to achieve (>= 0). If set to zero, the exact quantiles are computed, which could be very expensive. Note that values greater than 1 are accepted but give the same result as 1.

        Returns:
            the approximate quantiles at the given probabilities. If the input col is a string, the output is a list of floats. If the input col is a list or tuple of strings, the output is also a list, but each element in it is a list of floats, i.e., the output is a list of list of floats.
        """

        return self._data.approxQuantile(col=col,probabilities=probabilities,relativeError=relativeError)

    # def between(self, column_name, lowerBound, upperBound):
    #     """
    #     A boolean expression that is evaluated to true if the value of this expression is between the given columns.
    #     Args:
    #         column_name (str): name of the column
    #         lowerBound:
    #         upperBound:
    #     Examples:
    #         >>> ds.select(ds.timestamp, ds.between("column-name",2, 4)).show()
    #     Returns:
    #         DataStream object
    #     """
    #
    #     data = self._data[column_name].between(lowerBound=lowerBound, upperBound=upperBound)
    #     return DataStream(data=data, metadata=Metadata())

    # def cast(self, dataType, columnName):
    #     """
    #     Convert the column into type dataType (int, string, double, float).
    #     Args:
    #         dataType (str): new dataType of the column
    #         columnName (str): name of the column
    #     Examples:
    #         >>> ds.select(ds.col_name.cast("string").alias('col_name')).collect()
    #     Returns:
    #         DataStream object
    #     """
    #
    #     data = self._data[columnName].cast(dataType=dataType)
    #     return DataStream(data=data, metadata=Metadata())

    def colRegex(self,colName):
        """
        Selects column based on the column name specified as a regex and returns it as Column.

        Args:
            colName (str): column name specified as a regex.

        Returns:
            DataStream:

        Examples:
            >>> ds.colRegex("colName")
        """
        return DataStream(data=self._data.colRegex(colName=colName), metadata=Metadata())

    def collect(self):
        """
        Collect all the data to master node and return list of rows

        Returns:
            List: rows of all the dataframe

        Examples:
            >>> ds.collect()
        """
        return self._data.collect()

    def crossJoin(self, other):
        """
        Returns the cartesian product with another DataStream

        Args:
            other: Right side of the cartesian product.

        Returns:
            DataStream object with joined streams

        Examples:
            >>> ds.crossJoin(ds2.select("col_name")).collect()
        """
        data = self._data.crossJoin(other=other)
        return DataStream(data=data, metadata=Metadata())

    def crosstab(self, col1, col2):
        """
        Computes a pair-wise frequency table of the given columns. Also known as a contingency table. The number of distinct values for each column should be less than 1e4. At most 1e6 non-zero pair frequencies will be returned. The first column of each row will be the distinct values of col1 and the column names will be the distinct values of col2. The name of the first column will be $col1_$col2. Pairs that have no occurrences will have zero as their counts.

        Args:
            col1 (str): The name of the first column. Distinct items will make the first item of each row.
            col2 (str): The name of the second column. Distinct items will make the column names of the DataStream.

        Returns:
            DataStream object

        Examples:
            >>> ds.crosstab("col_1", "col_2")
        """
        data = self._data.crosstab(col1=col1, col2=col2)
        return DataStream(data=data, metadata=Metadata())

    def corr(self, col1, col2, method=None):
        """
        Calculates the correlation of two columns of a DataStream as a double value. Currently only supports the Pearson Correlation Coefficient.

        Args:
            col1 (str): The name of the first column
            col2 (str): The name of the second column
            method (str): The correlation method. Currently only supports “pearson”

        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Examples:
            >>> ds.corr("cal1", "col2", "pearson").collect()
        """

        data = self._data.corr(col1, col2, method)
        return DataStream(data=data, metadata=Metadata())

    def cov(self, col1, col2):
        """
        Calculate the sample covariance for the given columns, specified by their names, as a double value.

        Args:
            col1 (str): The name of the first column
            col2 (str): The name of the second column

        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Examples:
            >>> ds.cov("cal1", "col2", "pearson").collect()
        """

        data = self._data.cov(col1, col2)
        return DataStream(data=data, metadata=Metadata())

    def count(self):
        """
        Returns the number of rows in this DataStream.

        Examples:
            >>> ds.count()
        """
        return self._data.count()

    def distinct(self):
        """
        Returns a new DataStream containing the distinct rows in this DataStream.

        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Examples:
            >>> ds.distinct().count()
        """

        data = self._data.distinct()
        return DataStream(data=data, metadata=Metadata())

    def drop(self, *cols):
        """
        Returns a new Datastream that drops the specified column. This is a no-op if schema doesn’t contain the given column name(s).

        Args:
            *cols: a string name of the column to drop, or a Column to drop, or a list of string name of the columns to drop.

        Returns:
            Datastream:

        Examples:
            >>> ds.drop('col_name')
        """
        data = self._data.drop(*cols)
        return DataStream(data=data, metadata=Metadata())

    def describe(self, *cols):
        """
        Computes basic statistics for numeric and string columns. This include count, mean, stddev, min, and max. If no columns are given, this function computes statistics for all numerical or string columns.

        Args:
            *cols:

        Examples:
            >>> ds.describe(['col_name']).show()
            >>> ds.describe().show()
        """
        self._data.describe()

    def dropDuplicates(self, subset=None):
        """
        Return a new DataStream with duplicate rows removed, optionally only considering certain columns.

        Args:
            subset: optional list of column names to consider.

        Returns:
            Datastream:

        Examples:
            >>> ds.dropDuplicates().show()
            >>> # Example on how to use it with params
            >>> ds.dropDuplicates(['col_name1', 'col_name2']).show()
        """
        data = self._data.dropDuplicates(subset=subset)
        return DataStream(data=data, metadata=Metadata())

    def dropna(self, how='any', thresh=None, subset=None):
        """
        Returns a new DataStream omitting rows with null values.

        Args:
            how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its values are null.
            thresh: int, default None If specified, drop rows that have less than thresh non-null values. This overwrites the how parameter.
            subset: optional list of column names to consider.
        Returns:
            Datastream:

        Examples:
            >>> ds.dropna()
        """
        data = self._data.dropna(how=how, thresh=thresh, subset=subset)
        return DataStream(data=data, metadata=Metadata())

    def explain(self, extended=False):
        """
        Prints the (logical and physical) plans to the console for debugging purpose.

        Args:
            extended:  boolean, default False. If False, prints only the physical plan.

        Examples:
            >>> ds.explain()
        """
        self._data.explain()

    def exceptAll(self, other):
        """
        Return a new DataStream containing rows in this DataStream but not in another DataStream while preserving duplicates.

        Args:
            other: other DataStream object

        Returns:
            Datastream:

        Examples:
            >>> ds1.exceptAll(ds2).show()
        """
        data = self._data.exceptAll(other=other._data)
        return DataStream(data=data, metadata=Metadata())

    def fillna(self, value, subset=None):
        """
        Replace null values

        Args:
            value: int, long, float, string, bool or dict. Value to replace null values with. If the value is a dict, then subset is ignored and value must be a mapping from column name (string) to replacement value. The replacement value must be an int, long, float, boolean, or string.
            subset: optional list of column names to consider. Columns specified in subset that do not have matching data type are ignored. For example, if value is a string, and subset contains a non-string column, then the non-string column is simply ignored.

        Returns:
            Datastream:

        Examples:
            >>> ds.fill(50).show()
            >>> ds.fill({'col1': 50, 'col2': 'unknown'}).show()
        """
        data = self._data.fillna(value=value, subset=subset)
        return DataStream(data=data, metadata=Metadata())

    def repartition(self, numPartitions, *cols):
        """
        Returns a new DataStream partitioned by the given partitioning expressions. The resulting DataStream is hash partitioned.

        numPartitions can be an int to specify the target number of partitions or a Column. If it is a Column, it will be used as the first partitioning column. If not specified, the default number of partitions is used.

        Args:
            numPartitions:
            *cols:

        Returns:

        """
        data = self._data.repartition(numPartitions,*cols)
        return DataStream(data=data, metadata=Metadata())

    def filter(self, condition):
        """
        Filters rows using the given condition

        Args:
            condition: a Column of types.BooleanType or a string of SQL expression.

        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Examples:
            >>> ds.filter("age > 3")
            >>> df.filter(df.age > 3)

        """
        data = self._data.filter(condition)
        return DataStream(data=data, metadata=Metadata())

    def foreach(self, f):
        """
        Applies the f function to all Row of DataStream. This is a shorthand for df.rdd.foreach()

        Args:
            f: function

        Returns:
            DataStream object

        Examples:
            >>> def f(person):
            ...     print(person.name)
            >>> ds.foreach(f)
        """

        data = self._data.foreach(f)
        return DataStream(data=data, metadata=Metadata())

    def first(self):
        """
        Returns the first row as a Row.

        Returns:
            First row of a DataStream

        Examples:
            >>> ds.first()
        """

        return self._data.first()

    def freqItems(self, cols, support=None):
        """
        Finding frequent items for columns, possibly with false positives. Using the frequent element count algorithm described in “http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou”.

        Returns:
            DataStream:

        Examples:
            >>> ds.freqItems("col-name")
        """

        data = self._data.freqItems(cols=cols,support=support)
        return DataStream(data=data, metadata=Metadata())

    def groupby(self, *cols):
        """
        Groups the DataFrame using the specified columns, so we can run aggregation on them.

        Args:
            list of columns to group by. Each element should be a column name (string) or an expression (Column)

        Returns:
        """
        data = self._data.groupby(*cols)
        return DataStream(data=data, metadata=Metadata())

    def head(self, n=None):
        """
        Returns the first n rows.

        Args:
            n (int): default 1. Number of rows to return.

        Returns:
            If n is greater than 1, return a list of Row. If n is 1, return a single Row.

        Notes:
            This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.

        Examples:
            >>> ds.head(5)
        """
        return self._data.head(n=n)

    def intersect(self, other):
        """
        Return a new DataFrame containing rows only in both this frame and another frame. This is equivalent to INTERSECT in SQL.

        Args:
            other (int): DataStream object

        Returns:
            If n is greater than 1, return a list of Row. If n is 1, return a single Row.

        Examples:
            >>> ds.intersect(other=ds2)
        """
        data = self._data.intersect(other=other._data)
        return DataStream(data=data, metadata=Metadata())

    def intersectAll(self, other):
        """
        Return a new DataFrame containing rows in both this dataframe and other dataframe while preserving duplicates.

        Args:
            other (int): DataStream object

        Returns:
            If n is greater than 1, return a list of Row. If n is 1, return a single Row.

        Examples:
            >>> ds.intersectAll(ds2).show()
        """
        data = self._data.intersectAll(other=other._data)
        return DataStream(data=data, metadata=Metadata())

    def join(self, other, on=None, how=None):
        """
        Joins with another DataStream, using the given join expression.

        Args:
            other (DataStream):  Right side of the join
            on – a string for the join column name, a list of column names, a join expression (Column), or a list of Columns. If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides, and this performs an equi-join.
            how (str) – str, default inner. Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti.

        Examples:
            >>> ds.join(ds2, 'user', 'outer').show()

        Returns:
            DataStream object with joined streams
        """

        data = self._data.join(other=other._data, on=on, how=how)
        return DataStream(data=data, metadata=Metadata())

    def limit(self, num):
        """
        Limits the result count to the number specified.

        Args:
            num:

        Returns:
            Datastream:
        """
        data = self._data.limit(num=num)
        return DataStream(data=data, metadata=Metadata())

    def orderBy(self, *cols):
        """
        order by column name

        Args:
            *cols:

        Returns:
            Datastream:
        """
        data = self._data.orderBy(*cols)
        return DataStream(data=data, metadata=Metadata())

    def printSchema(self):
        """
        Prints out the schema in the tree format.

        Examples:
            >>> ds.printSchema()
        """
        self._data.printSchema()

    def replace(self, to_replace, value, subset=None):
        """
        Returns a new DataStream replacing a value with another value. Values to_replace and value must have the same type and can only be numerics, booleans, or strings. Value can have None. When replacing, the new value will be cast to the type of the existing column. For numeric replacements all values to be replaced should have unique floating point representation. In case of conflicts (for example with {42: -1, 42.0: 1}) and arbitrary replacement will be used.

        Args:
            to_replace: bool, int, long, float, string, list or dict. Value to be replaced. If the value is a dict, then value is ignored or can be omitted, and to_replace must be a mapping between a value and a replacement.
            value: bool, int, long, float, string, list or None. The replacement value must be a bool, int, long, float, string or None. If value is a list, value should be of the same length and type as to_replace. If value is a scalar and to_replace is a sequence, then value is used as a replacement for each item in to_replace.
            subset: optional list of column names to consider. Columns specified in subset that do not have matching data type are ignored. For example, if value is a string, and subset contains a non-string column, then the non-string column is simply ignored.

        Returns:
            Datastream:

        Examples:
            >>> ds.replace(10, 20).show()
            >>> ds.replace('some-str', None).show()
            >>> ds.replace(['old_val1', 'new_val1'], ['old_val2', 'new_val2'], 'col_name').show()
        """
        data = self._data.replace(to_replace, value, subset)
        return DataStream(data=data, metadata=Metadata())

    def select(self, *cols):
        """
        Projects a set of expressions and returns a new DataStream
        Args:
            cols(str): list of column names (string) or expressions (Column). If one of the column names is ‘*’, that column is expanded to include all columns in the current DataStream
        Returns:
            DataStream: this will return a new datastream object with selected columns
        Examples:
            >>> ds.select('*')
            >>> ds.select('name', 'age')
            >>> ds.select(ds.name, (ds.age + 10).alias('age'))
        """

        data = self._data.select(*cols)
        return DataStream(data=data, metadata=Metadata())

    def selectExpr(self, *expr):
        """
        This is a variant of select() that accepts SQL expressions. Projects a set of expressions and returns a new DataStream

        Args:
            expr(str):

        Returns:
            DataStream: this will return a new datastream object with selected columns

        Examples:
            >>> ds.selectExpr("age * 2")
        """

        data = self._data.selectExpr(*expr)
        return DataStream(data=data, metadata=Metadata())

    def sort(self, *cols, **kwargs):
        """
        Returns a new DataStream sorted by the specified column(s).

        Args:
            cols: list of Column or column names to sort by.
            ascending: boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.

        Returns:
            object: DataStream object

        Examples:
            >>> ds.sort("col_name", ascending=False)
        """
        data = self._data.sort(*cols, **kwargs)
        return DataStream(data=data, metadata=Metadata())

    def summary(self, *statistics):
        """
        Computes specified statistics for numeric and string columns. Available statistics are: - count - mean - stddev - min - max - arbitrary approximate percentiles specified as a percentage (eg, 75%) If no statistics are given, this function computes count, mean, stddev, min, approximate quartiles (percentiles at 25%, 50%, and 75%), and max.

        Args:
            *statistics:

        Examples:
            >>> ds.summary().show()
            >>> ds.summary("count", "min", "25%", "75%", "max").show()
            >>> # To do a summary for specific columns first select them:
            >>> ds.select("col1", "col2").summary("count").show()
        """
        self._data.summary()

    def take(self,num):
        """
        Returns the first num rows as a list of Row.

        Returns:
            Row(list): row(s) of a DataStream

        Examples:
            >>> ds.take()
        """

        return self._data.take(num=num)

    def toPandas(self):
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
            >>> new_ds = ds.toPandas()
            >>> new_ds.data.head()
        """
        pdf = self._data.toPandas()
        return pdf

    def union(self, other):
        """
        Return a new Datastream containing union of rows in this and another frame.

        This is equivalent to UNION ALL in SQL. To do a SQL-style set union (that does deduplication of elements), use this function followed by distinct().

        Also as standard in SQL, this function resolves columns by position (not by name).

        Args:
            other(DataStream):

        Returns:
            Datastream:

        Examples:
            >>> ds.union(ds2).collect()
        """
        data = self._data.union(other=other._data)
        return DataStream(data=data, metadata=Metadata())

    def unionByName(self, other):
        """
        Returns a new Datastream containing union of rows in this and another frame.

        This is different from both UNION ALL and UNION DISTINCT in SQL. To do a SQL-style set union (that does deduplication of elements), use this function followed by distinct().

        The difference between this function and union() is that this function resolves columns by name (not by position):

        Args:
            other(DataStream):

        Returns:
            Datastream:

        Examples:
            >>> ds.unionByName(ds2).show()
        """
        data = self._data.unionByName(other=other._data)
        return DataStream(data=data, metadata=Metadata())

    def where(self, condition):
        """
        where() is an alias for filter().

        Args:
            condition:

        Returns:
            Datastream:

        Examples:
            >>> ds.filter("age > 3").collect()
        """
        data = self._data.where(condition)
        return DataStream(data=data, metadata=Metadata())

    def withColumnRenamed(self, existing, new):
        """
        Returns a new DataStream by renaming an existing column. This is a no-op if schema doesn’t contain the given column name.

        Args:
            existing (str): string, name of the existing column to rename.
            new (str): string, new name of the column.

        Examples:
            >>> ds.withColumnRenamed('col_name', 'new_col_name')

        Returns:
            DataStream object with new column name(s)
        """

        data = self._data.withColumnRenamed(existing=existing, new=new)
        return DataStream(data=data, metadata=Metadata())

    def withColumn(self, colName, col):
        """
        Returns a new DataStream by adding a column or replacing the existing column that has the same name. The column expression must be an expression over this DataStream; attempting to add a column from some other datastream will raise an error.
        Args:
            colName (str): name of the new column.
            col: a Column expression for the new column.
        Examples:
            >>> ds.withColumn('col_name', ds.col_name + 2)
        """
        data = self._data.withColumn(colName=colName, col=col)
        return DataStream(data=data, metadata=Metadata())

    # !!!!                       END Wrapper for PySpark Methods                           !!!

    ##################### !!!!!!!!!!!!!!!! FEATURE COMPUTATION UDFS !!!!!!!!!!!!!!!!!!! ################################

    ### COMPUTE FFT FEATURES

    def compute_fourier_features(self, exclude_col_names: list = [], feature_names = ["fft_centroid", 'fft_spread', 'spectral_entropy', 'spectral_entropy_old', 'fft_flux',
            'spectral_folloff'], windowDuration: int = None, slideDuration: int = None,
                                 groupByColumnName: List[str] = [], startTime=None):
        """
        Transforms data from time domain to frequency domain.

        Args:
            exclude_col_names list(str): name of the columns on which features should not be computed
            feature_names list(str): names of the features. Supported features are fft_centroid, fft_spread, spectral_entropy, spectral_entropy_old, fft_flux, spectral_folloff
            windowDuration (int): duration of a window in seconds
            slideDuration (int): slide duration of a window
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided


        Returns:
            DataStream object with all the existing data columns and FFT features
        """
        eps = 0.00000001

        exclude_col_names.extend(["timestamp", "localtime", "user", "version"])

        data = self._data.drop(*exclude_col_names)

        df_column_names = data.columns

        basic_schema = StructType([
            StructField("timestamp", TimestampType()),
            StructField("localtime", TimestampType()),
            StructField("user", StringType()),
            StructField("version", IntegerType()),
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType())
        ])

        features_list = []
        for cn in df_column_names:
            for sf in feature_names:
                features_list.append(StructField(cn + "_" + sf, FloatType(), True))

        features_schema = StructType(basic_schema.fields + features_list)

        def stSpectralCentroidAndSpread(X, fs):
            """Computes spectral centroid of frame (given abs(FFT))"""
            ind = (np.arange(1, len(X) + 1)) * (fs / (2.0 * len(X)))

            Xt = X.copy()
            Xt = Xt / Xt.max()
            NUM = np.sum(ind * Xt)
            DEN = np.sum(Xt) + eps

            # Centroid:
            C = (NUM / DEN)

            # Spread:
            S = np.sqrt(np.sum(((ind - C) ** 2) * Xt) / DEN)

            # Normalize:
            C = C / (fs / 2.0)
            S = S / (fs / 2.0)

            return (C, S)

        def stSpectralFlux(X, Xprev):
            """
            Computes the spectral flux feature of the current frame
            ARGUMENTS:
                X:        the abs(fft) of the current frame
                Xpre:        the abs(fft) of the previous frame
            """
            # compute the spectral flux as the sum of square distances:

            sumX = np.sum(X + eps)
            sumPrevX = np.sum(Xprev + eps)
            F = np.sum((X / sumX - Xprev / sumPrevX) ** 2)

            return F

        def stSpectralRollOff(X, c, fs):
            """Computes spectral roll-off"""

            totalEnergy = np.sum(X ** 2)
            fftLength = len(X)
            Thres = c * totalEnergy
            # Ffind the spectral rolloff as the frequency position where the respective spectral energy is equal to c*totalEnergy
            CumSum = np.cumsum(X ** 2) + eps
            [a, ] = np.nonzero(CumSum > Thres)
            if len(a) > 0:
                mC = np.float64(a[0]) / (float(fftLength))
            else:
                mC = 0.0
            return (mC)
        
        def stSpectralEntropy(X, numOfShortBlocks=10):
            """Computes the spectral entropy"""
            L = len(X)  # number of frame samples
            Eol = np.sum(X ** 2)  # total spectral energy

            subWinLength = int(np.floor(L / numOfShortBlocks))  # length of sub-frame
            if L != subWinLength * numOfShortBlocks:
                X = X[0:subWinLength * numOfShortBlocks]

            subWindows = X.reshape(subWinLength, numOfShortBlocks,
                                   order='F').copy()  # define sub-frames (using matrix reshape)
            s = np.sum(subWindows ** 2, axis=0) / (Eol + eps)  # compute spectral sub-energies
            En = -np.sum(s * np.log2(s + eps))  # compute spectral entropy

            return En
        
        def spectral_entropy(data, sampling_freq, bands=None):

            psd = np.abs(np.fft.rfft(data)) ** 2
            psd /= np.sum(psd)  # psd as a pdf (normalised to one)

            if bands is None:
                power_per_band = psd[psd > 0]
            else:
                freqs = np.fft.rfftfreq(data.size, 1 / float(sampling_freq))
                bands = np.asarray(bands)

                freq_limits_low = np.concatenate([[0.0], bands])
                freq_limits_up = np.concatenate([bands, [np.Inf]])

                power_per_band = [np.sum(psd[np.bitwise_and(freqs >= low, freqs < up)])
                                  for low, up in zip(freq_limits_low, freq_limits_up)]

                power_per_band = power_per_band[power_per_band > 0]

            return -np.sum(power_per_band * np.log2(power_per_band))

        def fourier_features_pandas_udf(data, frequency: float = 16.0):

            Fs = frequency  # the sampling freq (in Hz)
            results = []
            # fourier transforms!
            # data_fft = abs(np.fft.rfft(data))

            X = abs(np.fft.fft(data))
            nFFT = int(len(X) / 2) + 1

            X = X[0:nFFT]  # normalize fft
            X = X / len(X)

            if "fft_centroid" or "fft_spread" in feature_names:
                C, S = stSpectralCentroidAndSpread(X, Fs)  # spectral centroid and spread
                if "fft_centroid" in feature_names:
                    results.append(C)
                if "fft_spread" in feature_names:
                    results.append(S)
            if "spectral_entropy" in feature_names:
                se = stSpectralEntropy(X)  # spectral entropy
                results.append(se)
            if "spectral_entropy_old" in feature_names:
                se_old = spectral_entropy(X, frequency)  # spectral flux
                results.append(se_old)
            if "fft_flux" in feature_names:
                flx = stSpectralFlux(X, X.copy())  # spectral flux
                results.append(flx)
            if "spectral_folloff" in feature_names:
                roff = stSpectralRollOff(X, 0.90, frequency)  # spectral rolloff
                results.append(roff)
            return pd.Series(results)

        @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
        def get_fft_features(df):
            timestamp = df['timestamp'].iloc[0]
            localtime = df['localtime'].iloc[0]
            user = df['user'].iloc[0]
            version = df['version'].iloc[0]
            start_time = timestamp
            end_time = df['timestamp'].iloc[-1]

            df.drop(exclude_col_names, axis=1, inplace=True)

            df_ff = df.apply(fourier_features_pandas_udf)
            df3 = df_ff.T
            pd.set_option('display.max_colwidth', -1)
            # split column into multiple columns
            #df3 = pd.DataFrame(df_ff.values.tolist(), index=df_ff.index)
            # print("**"*50)
            # print(type(df), type(df_ff), type(df3))
            # print(df)
            # print(df_ff)
            # print(df_ff.values.tolist())
            # print(df3)
            # print("**" * 50)
            # print("FEATURE-NAMES", feature_names)
            df3.columns = feature_names

            # multiple rows to one row
            output = df3.unstack().to_frame().sort_index(level=1).T
            output.columns = [f'{j}_{i}' for i, j in output.columns]

            basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time]],
                                    columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time'])
            #df.insert(loc=0, columns=, value=basic_cols)
            return basic_df.assign(**output)

        return self.compute(get_fft_features, windowDuration=windowDuration, slideDuration=slideDuration, groupByColumnName=groupByColumnName, startTime=startTime)
        #return DataStream(data=data._data, metadata=Metadata())

        ### COMPUTE STATISTICAL FEATURES

    def compute_statistical_features(self, exclude_col_names: list = [], feature_names = ['mean', 'median', 'stddev', 'variance', 'max', 'min', 'skew',
                         'kurt', 'sqr', 'zero_cross_rate'], windowDuration: int = None,
                                     slideDuration: int = None,
                                     groupByColumnName: List[str] = [], startTime=None):

        """
        Compute statistical features.

        Args:
            exclude_col_names list(str): name of the columns on which features should not be computed
            feature_names list(str): names of the features. Supported features are ['mean', 'median', 'stddev', 'variance', 'max', 'min', 'skew',
                         'kurt', 'sqr', 'zero_cross_rate'
            windowDuration (int): duration of a window in seconds
            slideDuration (int): slide duration of a window
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided


        Returns:
            DataStream object with all the existing data columns and FFT features
        """
        exclude_col_names.extend(["timestamp", "localtime", "user", "version"])

        data = self._data.drop(*exclude_col_names)

        df_column_names = data.columns

        basic_schema = StructType([
            StructField("timestamp", TimestampType()),
            StructField("localtime", TimestampType()),
            StructField("user", StringType()),
            StructField("version", IntegerType()),
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType())
        ])

        features_list = []
        for cn in df_column_names:
            for sf in feature_names:
                features_list.append(StructField(cn + "_" + sf, FloatType(), True))

        features_schema = StructType(basic_schema.fields + features_list)

        def calculate_zero_cross_rate(series):
            """
            How often the signal changes sign (+/-)
            """
            series_mean = np.mean(series)
            series = [v - series_mean for v in series]
            zero_cross_count = (np.diff(np.sign(series)) != 0).sum()
            return zero_cross_count / len(series)

        def get_sqr(series):
            sqr = np.mean([v * v for v in series])
            return sqr

        @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
        def get_stats_features_udf(df):
            results = []
            timestamp = df['timestamp'].iloc[0]
            localtime = df['localtime'].iloc[0]
            user = df['user'].iloc[0]
            version = df['version'].iloc[0]
            start_time = timestamp
            end_time = df['timestamp'].iloc[-1]

            df.drop(exclude_col_names, axis=1, inplace=True)

            if "mean" in feature_names:
                df_mean = df.mean()
                df_mean.index += '_mean'
                results.append(df_mean)

            if "median" in feature_names:
                df_median = df.median()
                df_median.index += '_median'
                results.append(df_median)

            if "stddev" in feature_names:
                df_stddev = df.std()
                df_stddev.index += '_stddev'
                results.append(df_stddev)

            if "variance" in feature_names:
                df_var = df.var()
                df_var.index += '_variance'
                results.append(df_var)

            if "max" in feature_names:
                df_max = df.max()
                df_max.index += '_max'
                results.append(df_max)

            if "min" in feature_names:
                df_min = df.min()
                df_min.index += '_min'
                results.append(df_min)

            if "skew" in feature_names:
                df_skew = df.skew()
                df_skew.index += '_skew'
                results.append(df_skew)

            if "kurt" in feature_names:
                df_kurt = df.kurt()
                df_kurt.index += '_kurt'
                results.append(df_kurt)

            if "zero_cross_rate" in feature_names:
                df_zero_cross_rate = df.apply(calculate_zero_cross_rate)
                df_zero_cross_rate.index += '_zero_cross_rate'
                results.append(df_zero_cross_rate)

            if "sqr" in feature_names:
                df_sqr = df.apply(get_sqr)
                df_sqr.index += '_sqr'
                results.append(df_sqr)

            output = pd.DataFrame(pd.concat(results)).T

            basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time]],
                                    columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time'])
            return basic_df.assign(**output)

        data = self.compute(get_stats_features_udf, windowDuration=windowDuration, slideDuration=slideDuration,
                            groupByColumnName=groupByColumnName, startTime=startTime)
        return DataStream(data=data._data, metadata=Metadata())

    ### COMPUTE Correlation and Mean Standard Error (MSE) FEATURES

    def compute_corr_mse_accel_gyro(self, exclude_col_names: list = [], accel_column_names:list=['accelerometer_x','accelerometer_y', 'accelerometer_z'], gyro_column_names:list=['gyroscope_y', 'gyroscope_x', 'gyroscope_z'], windowDuration: int = None,
                                     slideDuration: int = None,
                                     groupByColumnName: List[str] = [], startTime=None):
        """
        Compute correlation and mean standard error of accel and gyro sensors

        Args:
            exclude_col_names list(str): name of the columns on which features should not be computed
            accel_column_names list(str): name of accel data column
            gyro_column_names list(str): name of gyro data column
            windowDuration (int): duration of a window in seconds
            slideDuration (int): slide duration of a window
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided


        Returns:
            DataStream object with all the existing data columns and FFT features
        """
        feature_names = ["ax_ay_corr", 'ax_az_corr', 'ay_az_corr', 'gx_gy_corr', 'gx_gz_corr',
                         'gy_gz_corr', 'ax_ay_mse', 'ax_az_mse', 'ay_az_mse', 'gx_gy_mse', 'gx_gz_mse', 'gy_gz_mse']

        exclude_col_names.extend(["timestamp", "localtime", "user", "version"])

        data = self._data.drop(*exclude_col_names)

        basic_schema = StructType([
            StructField("timestamp", TimestampType()),
            StructField("localtime", TimestampType()),
            StructField("user", StringType()),
            StructField("version", IntegerType()),
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType())
        ])

        features_list = []
        for fn in feature_names:
            features_list.append(StructField(fn, FloatType(), True))

        features_schema = StructType(basic_schema.fields + features_list)

        @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
        def get_corr_mse_features_udf(df):
            timestamp = df['timestamp'].iloc[0]
            localtime = df['localtime'].iloc[0]
            user = df['user'].iloc[0]
            version = df['version'].iloc[0]
            start_time = timestamp
            end_time = df['timestamp'].iloc[-1]

            ax_ay_corr = df[accel_column_names[0]].corr(df[accel_column_names[1]])
            ax_az_corr = df[accel_column_names[0]].corr(df[accel_column_names[2]])
            ay_az_corr = df[accel_column_names[1]].corr(df[accel_column_names[2]])
            gx_gy_corr = df[gyro_column_names[0]].corr(df[gyro_column_names[1]])
            gx_gz_corr = df[gyro_column_names[0]].corr(df[gyro_column_names[2]])
            gy_gz_corr = df[gyro_column_names[1]].corr(df[gyro_column_names[2]])

            ax_ay_mse = ((df[accel_column_names[0]] - df[accel_column_names[1]]) ** 2).mean()
            ax_az_mse = ((df[accel_column_names[0]] - df[accel_column_names[2]]) ** 2).mean()
            ay_az_mse = ((df[accel_column_names[1]] - df[accel_column_names[2]]) ** 2).mean()
            gx_gy_mse = ((df[accel_column_names[0]] - df[accel_column_names[1]]) ** 2).mean()
            gx_gz_mse = ((df[accel_column_names[0]] - df[accel_column_names[2]]) ** 2).mean()
            gy_gz_mse = ((df[accel_column_names[1]] - df[accel_column_names[2]]) ** 2).mean()

            basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time, ax_ay_corr,
                                      ax_az_corr, ay_az_corr, gx_gy_corr, gx_gz_corr, gy_gz_corr, ax_ay_mse, ax_az_mse,
                                      ay_az_mse, gx_gy_mse, gx_gz_mse, gy_gz_mse]],
                                    columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time',
                                             "ax_ay_corr", 'ax_az_corr', 'ay_az_corr', 'gx_gy_corr', 'gx_gz_corr',
                                             'gy_gz_corr', 'ax_ay_mse', 'ax_az_mse', 'ay_az_mse', 'gx_gy_mse',
                                             'gx_gz_mse', 'gy_gz_mse'])
            return basic_df

        data = self.compute(get_corr_mse_features_udf, windowDuration=windowDuration, slideDuration=slideDuration,
                            groupByColumnName=groupByColumnName, startTime=startTime)
        return DataStream(data=data._data, metadata=Metadata())

    ## !!!! HELPER METHOD !!!!!! ##
    def _gen_metadata(self):
        schema = self._data.schema
        stream_metadata = Metadata()
        for field in schema.fields:
            stream_metadata.add_dataDescriptor(
                DataDescriptor().set_name(str(field.name)).set_type(str(field.dataType))
            )

        stream_metadata.add_module(
            ModuleMetadata().set_name("cerebralcortex.core.datatypes.datastream.DataStream").set_attribute("url",
                                                                                                       "hhtps://md2k.org").set_author(
                "Nasir Ali", "nasir.ali08@gmail.com"))

        return stream_metadata



    ###################### New Methods by Anand #########################

    def join_stress_streams(self, dataStream, propagation='forward'):
        """
        filter data

        Args:
            columnName (str): name of the column
            operator (str): basic operators (e.g., >, <, ==, !=)
            value (Any): if the columnName is timestamp, please provide python datatime object

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        combined_df = self._data.join(dataStream.data, on=['user', 'timestamp', 'localtime', 'version'],
                                      how='full').orderBy('timestamp')
        combined_filled = combined_df.withColumn("data_quality", F.last('data_quality', True).over(
            Window.partitionBy('user').orderBy('timestamp').rowsBetween(-sys.maxsize, 0)))
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
        return DataStream(data=windowed_df, metadata=Metadata())

    # def __str__(self):
    #     print("*"*10,"METADATA","*"*10)
    #     print(self.metadata)
    #     print("*"*10,"DATA","*"*10)
    #     print(self._data)
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
    s = str(y) + u + str(m) + u + str(d) + u + str(h)  # + u + str(mi)

    return s


windowing_udf = udf(get_window, StringType())

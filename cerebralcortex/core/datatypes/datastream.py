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

    # !!!!                              FILTERING METHODS                           !!!

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
            >>> ds.na.replace(10, 20).show()
            >>> ds.na.replace('some-str', None).show()
            >>> ds.na.replace(['old_val1', 'new_val1'], ['old_val2', 'new_val2'], 'col_name').show()

        """
        data = self._data.replace(to_replace, value, subset)
        return DataStream(data=data, metadata=Metadata())

    def limit(self, num):
        """
        Limits the result count to the number specified.

        Args:
            num:
            **kwargs:

        Returns:
            Datastream:
        """
        data = self._data.limit(num=num)
        return DataStream(data=data, metadata=Metadata())

    def where(self, condition):
        """
        where() is an alias for filter().

        Args:
            condition:

        Returns:
            Datastream:
        """
        data = self._data.where(condition)
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
            >>> ds.na.drop()

        """
        data = self._data.dropDuplicates(subset=subset)
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
            >>> ds.na.fill(50).show()
            >>> ds.na.fill({'col1': 50, 'col2': 'unknown'}).show()

        """
        data = self._data.fillna(value=value, subset=subset)
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

    def dtypes(self):
        """
        Returns all column names and their data types as a list.

        Examples:
            >>> ds.dtypes()

        """
        return self._data.dtypes

    def join(self, other, on=None, how=None):
        """
        Joins with another DataStream, using the given join expression.

        Args:
            other (DataStream):  Right side of the join
            on – a string for the join column name, a list of column names, a join expression (Column), or a list of Columns. If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides, and this performs an equi-join.
            how (str) – str, default inner. Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti.

        Examples:
            >>> ds.join(ds2, 'user', 'outer').select('user', 'col1', 'col2')

        Returns:
            DataStream object with joined streams
        """

        data = self._data.join(other=other._data, on=on, how=how)
        return DataStream(data=data, metadata=Metadata())

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

    def crosstab(self, f):
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

    def between(self, lowerBound, upperBound):
        """
        A boolean expression that is evaluated to true if the value of this expression is between the given columns.

        Args:
            lowerBound:
            upperBound:

        Examples:
            >>> ds.select(ds.timestamp, ds.data.col1.between(2, 4)).show()

        Returns:
            DataStream object
        """

        data = self._data.between(lowerBound=lowerBound, upperBound=upperBound)
        return DataStream(data=data, metadata=Metadata())

    def cast(self, dataType, columnName):
        """
        Convert the column into type dataType (int, string, double, float).

        Args:
            dataType (str): new dataType of the column
            columnName (str): name of the column

        Examples:
            >>> ds.select(ds.col_name.cast("string").alias('col_name')).collect()

        Returns:
            DataStream object
        """

        data = self._data[columnName].cast(dataType=dataType)
        return DataStream(data=data, metadata=Metadata())

    def filter(self,  condition):
        """
        Filters rows using the given condition

        Args:
            condition: a Column of types.BooleanType or a string of SQL expression.

        Examples:
            >>> ds.filter("age > 3")
            >>> df.filter(df.age > 3)

        Returns:
            DataStream: this will return a new datastream object with blank metadata
        """
        data = self._data.filter(condition)
        return DataStream(data=data, metadata=Metadata())

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

    def groupby(self, *cols):
        """
        Groups the DataFrame using the specified columns, so we can run aggregation on them.

        Args:
            list of columns to group by. Each element should be a column name (string) or an expression (Column)

        Returns:

        """
        data = self._data.groupby(*cols)
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
            >>> df.agg(F.min(df.age)).collect()
        """
        data = self._data.agg(*exprs)
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
            >>> ds.select(df.name, (df.age + 10).alias('age'))
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
        Run an algorithm. This method supports running an udf method on windowed data

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

    def sort(self, *cols, **kwargs):
        """
        Returns a new DataStream sorted by the specified column(s).

        Args:
            cols: list of Column or column names to sort by.
            ascending: boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.

        Returns:
            object: DataStream object

        Examples:
            >>> ds.sort(df.age.desc())
            >>> ds.sort("age", ascending=False)
            >>> ds.orderBy(df.age.desc())

        """
        data = self._data.sort(*cols, **kwargs)
        return DataStream(data=data, metadata=Metadata())

    def alias(self, *alias, **kwargs):
        """
        Returns this column aliased with a new name or names (in the case of expressions that return more than one column, such as explode)

        Args:
            alias: strings of desired column names (collects all positional arguments passed)
            metadata: a dict of information to be stored in metadata attribute of the corresponding :class: StructField (optional, keyword only argument)

        Returns:
            object: DataStream object

        Examples:
            >>> ds.select(df.age.alias("age2"))
            >>> ds.select(df.age.alias("age3", metadata={'max': 99})).schema['age3'].metadata['max']

        """
        data = self._data.alias(*alias, **kwargs)
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

    def show(self, n=20, truncate=True, vertical=False):
        """
        Prints the first n rows to the console.

        Args:
            n: Number of rows to show.
            truncate: If set to True, truncate strings longer than 20 chars by default. If set to a number greater than one, truncates long strings to length truncate and align cells right.
            vertical: If set to True, print output rows vertically (one line per column value).

        Examples:
            >>> ds.show(truncate=3)
            >>> ds.show(vertical=True)
        """

        self._data.show(n=n, truncate=truncate, vertical=vertical)

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



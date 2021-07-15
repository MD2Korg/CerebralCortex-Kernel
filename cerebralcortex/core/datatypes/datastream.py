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

import re
import sys
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.window import Window

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata


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

        if isinstance(data, DataFrame):
            super(self.__class__, self).__init__(data._jdf, data.sql_ctx)

        if self._metadata is not None and not isinstance(self.metadata,list) and len(self.metadata.data_descriptor)==0 and data is not None:
            self.metadata = self._gen_metadata()

    # !!!!                       Disable some of dataframe operations                           !!!
    def write(self):
        raise NotImplementedError

    def writeStream(self):
        raise NotImplementedError

    def get_metadata(self) -> Metadata:
        """
        get stream metadata

        Returns:
            Metadata: single version of a stream
        Raises:
            Exception: if specified version is not available for the stream

        """

        return self._metadata

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

    # !!!!                              WINDOWING METHODS                           !!!

    def window(self, windowDuration: int = None, groupByColumnName: List[str] = [],
               slideDuration: int = None, startTime=None, preserve_ts=False):
        """
        Window data into fixed length chunks. If no columnName is provided then the windowing will be performed on all the columns.

        Args:
            windowDuration (int): duration of a window in seconds
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            slideDuration (int): slide duration of a window
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
            preserve_ts (bool): setting this to True will return timestamps of corresponding to each windowed value
        Returns:
            DataStream: this will return a new datastream object with blank metadata
        Note:
            This windowing method will use collect_list to return values for each window. collect_list is not optimized.

        """
        if slideDuration:
            slideDuration = str(slideDuration) + " seconds"

        groupbycols = ["user", "version"]

        if len(groupByColumnName) > 0:
            groupbycols.extend(groupByColumnName)

        groupbycols = list(set(groupbycols))

        if windowDuration:
            windowDuration = str(windowDuration) + " seconds"
            win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration,
                           startTime=startTime)
            groupbycols.append(win)

        data = self._data.groupBy(groupbycols)

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


    def compute(self, udfName, schema=None, windowDuration: int = None, slideDuration: int = None,
                      groupByColumnName: List[str] = [], startTime=None):
        """
        Run an algorithm. This method supports running an udf method on windowed data

        Args:
            udfName: Name of the algorithm. The function should take a `pandas.DataFrame` and return another
                    `pandas.DataFrame`. For each group, all columns are passed together as a `pandas.DataFrame`
                    to the user-function and the returned `pandas.DataFrame` are combined as a
                    :class:`DataStream`.

            schema: The `schema` should be a :class:`StructType` describing the schema of the returned
                    `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
                    the field names in the defined schema if specified as strings, or match the
                    field data types by position if not strings, e.g. integer indices.
                    The length of the returned `pandas.DataFrame` can be arbitrary. Acceptable datatypes are "NullType", "StringType", "BinaryType", "BooleanType", "DateType", "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType", "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"

            windowDuration (int): duration of a window in seconds
            slideDuration (int): slide duration of a window
            groupByColumnName List[str]: groupby column names, for example, groupby user, col1, col2
            startTime (datetime): The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals. For example, in order to have hourly tumbling windows that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide startTime as 15 minutes. First time of data will be used as startTime if none is provided
        Returns:
            DataStream: this will return a new datastream object with blank metadata

        Examples:
            >>> def normalize(pdf):
            ...     v = pdf.v
            ...     return pdf.assign(v=(v - v.mean()) / v.std())
            # acceptable data types for schem are - "null", "string", "binary", "boolean",
            # "date", "timestamp", "decimal", "double", "float", "byte", "integer",
            # "long", "short", "array", "map", "structfield", "struct"
            >>> ds.groupby("timestamp").applyInPandas(
            ...     normalize, schema="id long, v double").show()
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

            if schema is None:
                data = self._data.groupBy(groupbycols).apply(udfName)
            else:
                data = self._data.groupBy(groupbycols).applyInPandas(func=udfName, schema=schema)

        return DataStream(data=data, metadata=Metadata())

    def _get_column_names(self, columnNames: List[str], methodName: str, preserve_ts: bool = False):
        """
        Get data column names and build expression for pyspark aggregate method

        Args:
            columnNames(List[str]): get all column names expression if columnName is empty
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

        if columnNames:
            if isinstance(columns, str):
                columns = [columnNames]
            elif isinstance(columns, list):
                columns = columnNames
        else:
            columns = list(set(columns) - set(black_list_column))

        exprs = {x: methodName for x in columns}
        return exprs


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

    def applyInPandas(self, func, schema):
        """
        The function should take a `pandas.DataFrame` and return another
        `pandas.DataFrame`. For each group, all columns are passed together as a `pandas.DataFrame`
        to the user-function and the returned `pandas.DataFrame` are combined as a
        `DataFrame`.

        The `schema` should be a `StructType` describing the schema of the returned
        `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pandas.DataFrame` can be arbitrary.

        Args:
            func: a Python native function that takes a `pandas.DataFrame`, and outputs a `pandas.DataFrame`.
            schema: :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Returns:

        """
        return DataStream(data=self._data.applyInPandas(func=func, schema=schema), metadata=Metadata())

    def apply(self, udf):
        """

        Args:
            udf: :func:`pyspark.sql.functions.pandas_udf`
            a grouped map user-defined function returned by
            :func:`pyspark.sql.functions.pandas_udf`.

        Examples:
            >>> ds.groupby("timestamp").apply(normalize).show()
        Returns:

        """

        return DataStream(data=self._data.apply(udf=udf), metadata=Metadata())

    def cogroup(self, other):
        """
        Cogroups this group with another group so that we can run cogrouped operations.

        Returns:

        """

        return DataStream(data=self._data.cogroup(other=other), metadata=Metadata())

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

    #TODO: missing param for cov algo = pearson
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
        return self._data.describe()

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

    def explain(self,  extended=None, mode=None):
        """
        Prints the (logical and physical) plans to the console for debugging purpose.

        Args:
            extended:  bool, optional
            default ``False``. If ``False``, prints only the physical plan.
            When this is a string without specifying the ``mode``, it works as the mode is
            specified.

            mode: str, optional
            specifies the expected output format of plans.
            * ``simple``: Print only a physical plan.
            * ``extended``: Print both logical and physical plans.
            * ``codegen``: Print a physical plan and generated codes if they are available.
            * ``cost``: Print a logical plan and statistics if they are available.
            * ``formatted``: Split explain output into two sections: a physical plan outline \
                and node details.

        Examples:
            >>> ds.explain()
        """
        self._data.explain( extended=extended, mode=mode)

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
        Groups the DataFrame using the specified columns, so we can run aggregation on them. This method will return pyspark.sql.GroupedData object.

        Args:
            list of columns to group by. Each element should be a column name (string) or an expression (Column)

        Returns:
        """
        data = self._data.groupby(*cols)
        return data

    def show(self, n=20, truncate=True, vertical=False):
        """

        Args:
            n: Number of rows to show.
            truncate: If set to ``True``, truncate strings longer than 20 chars by default.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.
            vertical: If set to ``True``, print output rows vertically (one line
            per column value).

        Returns:

        """
        from pyspark.sql.group import GroupedData
        if isinstance(self._data, GroupedData):
            raise Exception(
                "show is not callable on windowed/grouped data.")
        self._data.show(n=n, truncate=truncate, vertical=vertical)

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

    def mapInPandas(self, func, schema):
        """
        Maps an iterator of batches in the current :class:`DataFrame` using a Python native
        function that takes and outputs a pandas DataFrame, and returns the result as a
        :class:`DataFrame`.
        The function should take an iterator of `pandas.DataFrame`\\s and return
        another iterator of `pandas.DataFrame`\\s. All columns are passed
        together as an iterator of `pandas.DataFrame`\\s to the function and the
        returned iterator of `pandas.DataFrame`\\s are combined as a :class:`DataFrame`.
        Each `pandas.DataFrame` size can be controlled by
        `spark.sql.execution.arrow.maxRecordsPerBatch`.

        Args:
            func: function a Python native function that takes an iterator of `pandas.DataFrame`, and
                outputs an iterator of `pandas.DataFrame`.

            schema: :class:`pyspark.sql.types.DataType` or str
                the return type of the `func` in PySpark. The value can be either a
                :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Returns:

        Examples:
            >>> def filter_func(iterator):
            ...     for pdf in iterator:
            ...         yield pdf[pdf.id == 1]
            >>> ds.mapInPandas(filter_func, ds.schema).show()
        """

        return DataStream(data=self._data.mapInPandas(func=func, schema=schema), metadata=Metadata())

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

    def randomSplit(self, weights, seed=None):
        """
        Randomly splits this :class:`DataFrame` with the provided weights.

        Args:
            weights: list of doubles as weights with which to split the :class:`DataFrame`.
            Weights will be normalized if they don't sum up to 1.0.
            seed: int, optional
            The seed for sampling.

        Returns:

        Examples:
            >>> splits = ds.randomSplit([1.0, 2.0], 24)
            >>> splits[0].count()
        """
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
        self._data.summary(*statistics).show(truncate=False)

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

    def transform(self, func):
        """
        Returns a new :class:`DataFrame`. Concise syntax for chaining custom transformations

        Args:
            func: a function that takes and returns a :class:`DataFrame`.

        Examples:
            >>> def cast_all_to_int(input_df):
            ...     return input_df.select([col(col_name).cast("int") for col_name in input_df.columns])
            >>> def sort_columns_asc(input_df):
            ...     return input_df.select(*sorted(input_df.columns))
            >>> ds.transform(cast_all_to_int).transform(sort_columns_asc).show()
        """

        return DataStream(data=self._data.transform(func=func), metadata=Metadata())

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


    ## !!!! HELPER METHOD !!!!!! ##
    def _gen_metadata(self):
        from pyspark.sql.group import GroupedData
        if isinstance(self._data, GroupedData):
            return Metadata()
        else:
            schema = self._data.schema
            stream_metadata = Metadata()
            for field in schema.fields:
                stream_metadata.add_dataDescriptor(
                    DataDescriptor().set_name(str(field.name)).set_type(str(field.dataType))
                )

            stream_metadata.add_module(
                ModuleMetadata().set_name("cerebralcortex.core.datatypes.datastream.DataStream").set_attribute("url",
                                                                                                           "https://md2k.org").set_author(
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

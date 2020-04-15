from typing import List

import numpy as np
import pandas as pd

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
# from pyspark.sql.functions import pandas_udf,PandasUDFType
from pyspark.sql.types import StructType


def magnitude(ds, col_names=[]):
    """
    Compute magnitude of columns
    Args:
        ds (DataStream):
        col_names (list[str]): column names

    Returns:
        DataStream

    """
    if len(col_names) < 1:
        raise Exception("col_names param is missing.")

    tmp = ""
    for col_name in col_names:
        tmp += 'F.col("' + col_name + '")*F.col("' + col_name + '")+'
    tmp = tmp.rstrip("+")

    data = ds._data.withColumn("magnitude", F.sqrt(eval(tmp)))
    return DataStream(data=data, metadata=Metadata())


# stat
def interpolate(ds, freq=16, method='linear', axis=0, limit=None, inplace=False,
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
    schema = ds._data.schema
    sample_freq = 1000 / freq

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def interpolate_data(pdf):
        pdf.set_index("timestamp", inplace=True)
        pdf = pdf.resample(str(sample_freq) + "ms").bfill(limit=1).interpolate(method=method, axis=axis, limit=limit,
                                                                               inplace=inplace,
                                                                               limit_direction=limit_direction,
                                                                               limit_area=limit_area, downcast=downcast)
        pdf.ffill(inplace=True)
        pdf.reset_index(drop=False, inplace=True)
        pdf.sort_index(axis=1, inplace=True)
        return pdf

    data = ds._data.groupby(["user", "version"]).apply(interpolate_data)
    return DataStream(data=data, metadata=Metadata())


def statistical_features(ds, exclude_col_names: list = [],
                         feature_names=['mean', 'median', 'stddev', 'variance', 'max', 'min', 'skew',
                                        'kurt', 'sqr'], windowDuration: int = None,
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

    data = ds._data.drop(*exclude_col_names)

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

        if "sqr" in feature_names:
            df_sqr = df.apply(get_sqr)
            df_sqr.index += '_sqr'
            results.append(df_sqr)

        output = pd.DataFrame(pd.concat(results)).T

        basic_df = pd.DataFrame([[timestamp, localtime, user, int(version), start_time, end_time]],
                                columns=['timestamp', 'localtime', 'user', 'version', 'start_time', 'end_time'])
        return basic_df.assign(**output)

    data = ds.compute(get_stats_features_udf, windowDuration=windowDuration, slideDuration=slideDuration,
                      groupByColumnName=groupByColumnName, startTime=startTime)
    return DataStream(data=data._data, metadata=Metadata())

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

import numpy as np
import pandas as pd
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from sklearn.cluster import DBSCAN

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.group import GroupedData
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, DoubleType,StringType,FloatType, TimestampType, IntegerType

from cerebralcortex.util.helper_methods import get_study_names
from pyspark.sql.functions import pandas_udf, PandasUDFType

from pyspark.sql.functions import minute, second, mean, window
from pyspark.sql import functions as F
import numpy as np
import pandas as pd
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from typing import List
import numpy as np
from scipy import signal
import pandas as pd
import pandas as pd, numpy as np, os, csv, glob
from math import radians, cos, sin, asin, sqrt
from datetime import timedelta


# def cluster_gps(ds, epsilon_constant=10, latitude=0, longitude=1, gps_accuracy_threashold=41.0, km_per_radian=6371.0088,
#                 geo_fence_distance=2, minimum_points_in_cluster=50):
#     def get_centermost_point(cluster: object) -> object:
#         """
#
#         :param cluster:
#         :return:
#         :rtype: object
#         """
#         centroid = (
#             MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
#         centermost_point = min(cluster, key=lambda point: great_circle(point,
#                                                                        centroid).m)
#         return tuple(centermost_point)
#
#     schema = StructType([
#         StructField("timestamp", TimestampType()),
#         StructField("localtime", TimestampType()),
#         StructField("user", StringType()),
#         StructField("version", IntegerType()),
#
#         StructField("latitude", FloatType()),
#         StructField("longitude", FloatType())
#     ])
#
#     @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
#     def gps_clusters(data: object) -> object:
#         """
#         Computes the clusters
#
#         :rtype: object
#         :param list data: list of interpolated gps data
#         :param float geo_fence_distance: Maximum distance between points in a
#         cluster
#         :param int min_points_in_cluster: Minimum number of points in a cluster
#         :return: list of cluster-centroids coordinates
#         """
#         # geo_fence_distance = geo_fence_distance
#         min_points_in_cluster = minimum_points_in_cluster
#
#         data = data[data.accuracy < gps_accuracy_threashold]
#         try:
#             timestamp = data.timestamp.iloc[0]
#             localtime = data.localtime.iloc[0]
#             user = data.user.iloc[0]
#             version = data.version.iloc[0]
#             dataframe = pd.DataFrame(
#                 {'latitude': data.latitude, 'longitude': data.longitude})
#             coords = dataframe.as_matrix(columns=['latitude', 'longitude'])
#
#             epsilon = geo_fence_distance / (
#                     epsilon_constant * km_per_radian)
#             db = DBSCAN(eps=epsilon, min_samples=min_points_in_cluster,
#                         algorithm='ball_tree', metric='haversine').fit(
#                 np.radians(coords))
#             cluster_labels = db.labels_
#             num_clusters = len(set(cluster_labels))
#             clusters = pd.Series(
#                 [coords[cluster_labels == n] for n in range(-1, num_clusters)])
#             clusters = clusters.apply(lambda y: np.nan if len(y) == 0 else y)
#             clusters.dropna(how='any', inplace=True)
#             centermost_points = clusters.map(get_centermost_point)
#             centermost_points = np.array(centermost_points)
#             all_centroid = []
#             for cols in centermost_points:
#                 cols = np.array(cols)
#                 cols.flatten()
#                 cs = ([timestamp, localtime, user, version, cols[latitude], cols[longitude]])
#                 all_centroid.append(cs)
#             df = pd.DataFrame(all_centroid,
#                               columns=["timestamp", "localtime", "user", "version", 'latitude', 'longitude'])
#             return df
#         except:
#             pass
#
#     # check if datastream object contains grouped type of DataFrame
#     if not isinstance(ds._data, GroupedData):
#         raise Exception(
#             "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")
#
#     data = ds._data.apply(gps_clusters)
#     return DataStream(data=data, metadata=Metadata())

def impute_gps_data(ds, accuracy_threashold=100):
    schema = ds._data.schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def gps_imputer(data):
        data = data.sort_values('localtime').reset_index(drop=True)
        data['latitude'][data.accuracy > accuracy_threashold] = np.nan
        data['longitude'][data.accuracy > accuracy_threashold] = np.nan
        data = data.fillna(method='ffill').dropna()
        return data

    data = ds._data.apply(gps_imputer)
    return DataStream(data=data, metadata=Metadata())



def cluster_gps(ds, epsilon_constant = 1000, km_per_radian = 6371.0088, geo_fence_distance = 50, minimum_points_in_cluster = 200):
    '''

    Args:
        ds (DataStream): datastream object
        minimum_haversine_distance: in KM
        minimum_duration: in seconds
        minimum_gpspoints:

    Returns:

    '''

    features_list = [StructField('centroid_longitude', DoubleType()),
                     StructField('centroid_latitude', DoubleType()),
                     StructField('labels', IntegerType())]
    schema = StructType(ds._data.schema.fields + features_list)
    column_names = [a.name for a in schema.fields]

    def get_centermost_point(cluster: object) -> object:
        """
        :param cluster:
        :return:
        :rtype: object
        """
        centroid = (
            MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centermost_point = min(cluster, key=lambda point: great_circle(point,
                                                                       centroid).m)
        return tuple(centermost_point)



    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def gps_clustering(data):
        if data.shape[0] < minimum_points_in_cluster*2:
            return pd.DataFrame([], columns=column_names)


        coords = np.float64(data[['latitude', 'longitude']].values)

        epsilon = geo_fence_distance / (
                epsilon_constant * km_per_radian)

        db = DBSCAN(eps=epsilon, min_samples= minimum_points_in_cluster,
                    algorithm='ball_tree', metric='haversine').fit(
            np.radians(coords))

        data['labels'] = db.labels_
        cluster_labels = db.labels_
        clusters = pd.Series(
            [coords[cluster_labels == n] for n in np.unique(cluster_labels)])

        cluster_names = np.array([n for n in np.unique(cluster_labels)])
        centermost_points = clusters.map(get_centermost_point)
        centermost_points = np.array(centermost_points)

        all_dict = []
        for i, col in enumerate(cluster_names):
            cols = np.array(centermost_points[i])
            cols.flatten()
            all_dict.append([col, cols[0], cols[1]])

        temp_df = pd.DataFrame(all_dict, columns=['labels', 'centroid_latitude', 'centroid_longitude'])
        data = pd.merge(data, temp_df, how='left', left_on=['labels'], right_on=['labels'])
        return data


    data = ds._data.apply(gps_clustering)
    return DataStream(data=data, metadata=Metadata())

def stay_time_graph_in_gps_cluster(ds, minimum_gpspoints = 5):
    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('start_localtime', TimestampType()),
                         StructField('end_localtime', TimestampType()),
                         StructField('user', StringType()),
                         StructField('version', IntegerType()),
                         StructField('day', StringType()),
                         StructField('centroid_id', IntegerType()),
                         StructField('latitude', DoubleType()),
                         StructField('longitude', DoubleType()),
                         StructField('time_diff', DoubleType()),
                         StructField('hour_of_day', DoubleType())])

    pdf_schema = [a.name for a in schema.fields]
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def timebased_clustering(data):
        if data.shape[0] < minimum_gpspoints:
            return pd.DataFrame([], columns=pdf_schema)
        data = data.sort_values('localtime').reset_index(drop=True)
        acc_ltime = []
        data_all = []
        for i, row in data.iterrows():
            if i == 0 or i == data.shape[0] - 1:
                k = i
                start_date_all = row['time']
            else:
                if row['labels'] == data['labels'].iloc[k]:
                    acc_ltime.append(row['localtime'])
                else:
                    if len(acc_ltime) > minimum_gpspoints:
                        end_date_all = data['time'].iloc[i]
                        if end_date_all - start_date_all > minimum_gpspoints:
                            data_all.append(
                                [data['timestamp'].iloc[int((k + i) / 2)], data['localtime'].iloc[int((k + i) / 2)],
                                 acc_ltime[0], acc_ltime[-1],
                                 row['user'], row['version'], row['day'], data['labels'].iloc[k],
                                 data['centroid_latitude'].iloc[k], data['centroid_longitude'].iloc[k],
                                 (end_date_all - start_date_all) / 60, data['hour_of_day'].iloc[k]])

                    k = i + 1
                    start_date_all = data['time'].iloc[i + 1]
                    acc_ltime = []
        return pd.DataFrame(data_all, columns=pdf_schema)

    data = ds._data.apply(timebased_clustering)
    return DataStream(data=data, metadata=Metadata())
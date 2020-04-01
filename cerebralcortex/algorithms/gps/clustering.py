# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>, Md Azim Ullah <mullah@memphis.edu>
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
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from sklearn.cluster import DBSCAN
from pyspark.sql.types import StructField, StructType, DoubleType,StringType, TimestampType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
import pandas as pd, numpy as np
from scipy.spatial import ConvexHull


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




def cluster_gps(ds, epsilon_constant = 1000,
                km_per_radian = 6371.0088,
                geo_fence_distance = 50,
                minimum_points_in_cluster = 1,
                latitude_column_name = 'latitude',
                longitude_column_name = 'longitude'):
    '''

    Args:
        ds (DataStream): datastream object
        minimum_haversine_distance: in KM
        minimum_duration: in seconds
        minimum_gpspoints:

    Returns:

    '''
    columns = ds.columns
    centroid_id_name = 'centroid_id'
    features_list = [StructField('centroid_longitude', DoubleType()),
                     StructField('centroid_latitude', DoubleType()),
                     StructField('centroid_id', IntegerType()),
                     StructField('centroid_area', DoubleType())]
    centroid_names = [a.name for a in features_list]
    for c in centroid_names:
        if c in columns:
            ds = ds.drop(*[c])
    schema = StructType(ds._data.schema.fields + features_list)
    column_names = [a.name for a in schema.fields]

    def reproject(latitude, longitude):
        from math import pi, cos, radians
        earth_radius = 6371009 # in meters
        lat_dist = pi * earth_radius / 180.0

        y = [lat * lat_dist for lat in latitude]
        x = [long * lat_dist * cos(radians(lat))
             for lat, long in zip(latitude, longitude)]
        return np.column_stack((x, y))


    def get_centermost_point(cluster: np.ndarray) -> object:
        """
        :param cluster:
        :return:
        :rtype: object
        """
        try:
            if cluster.shape[0]>4:
                points_project = reproject(cluster[:,0],cluster[:,1])
                hull = ConvexHull(points_project)
                area = hull.area
            else:
                area = 1
        except:
            area = 1
        centroid = (
            MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centermost_point = min(cluster, key=lambda point: great_circle(point,
                                                                       centroid).m)
        return list(centermost_point) + [area]

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def gps_clustering(data):
        if data.shape[0] < minimum_points_in_cluster*2:
            return pd.DataFrame([], columns=column_names)


        coords = np.float64(data[[latitude_column_name, longitude_column_name]].values)

        epsilon = geo_fence_distance / (
                epsilon_constant * km_per_radian)

        db = DBSCAN(eps=epsilon, min_samples= minimum_points_in_cluster,
                    algorithm='ball_tree', metric='haversine').fit(
            np.radians(coords))

        data[centroid_id_name] = db.labels_
        cluster_labels = db.labels_
        clusters = pd.Series(
            [coords[cluster_labels == n] for n in np.unique(cluster_labels)])

        cluster_names = np.array([n for n in np.unique(cluster_labels)])
        centermost_points = clusters.map(get_centermost_point)
        centermost_points = np.array(centermost_points)

        all_dict = []
        for i, col in enumerate(cluster_names):
            cols = np.array(centermost_points[i])
            all_dict.append([col, cols[0], cols[1], cols[2]])

        temp_df = pd.DataFrame(all_dict, columns=[centroid_id_name, 'centroid_latitude', 'centroid_longitude', 'centroid_area'])
        data = pd.merge(data, temp_df, how='left', left_on=[centroid_id_name], right_on=[centroid_id_name])
        return data


    data = ds._data.groupBy('version').apply(gps_clustering)
    return DataStream(data=data, metadata=Metadata())
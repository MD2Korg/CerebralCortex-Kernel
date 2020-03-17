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
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.group import GroupedData
from shapely.geometry.multipoint import MultiPoint
from sklearn.cluster import DBSCAN
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata

from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType

def cluster_gps(ds, epsilon_constant = 10, latitude = 0,longitude = 1, gps_accuracy_threashold = 41.0,km_per_radian = 6371.0088, geo_fence_distance = 2,minimum_points_in_cluster = 50):

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


    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("user", StringType()),
        StructField("version", IntegerType()),

        StructField("latitude", FloatType()),
        StructField("longitude", FloatType())
    ])


    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def gps_clusters(data: object) -> object:
        """
        Computes the clusters

        :rtype: object
        :param list data: list of interpolated gps data
        :param float geo_fence_distance: Maximum distance between points in a
        cluster
        :param int min_points_in_cluster: Minimum number of points in a cluster
        :return: list of cluster-centroids coordinates
        """
        #geo_fence_distance = geo_fence_distance
        min_points_in_cluster = minimum_points_in_cluster

        data = data[data.accuracy < gps_accuracy_threashold]
        try:
            timestamp = data.timestamp.iloc[0]
            localtime = data.localtime.iloc[0]
            user = data.user.iloc[0]
            version = data.version.iloc[0]
            dataframe = pd.DataFrame(
                {'latitude': data.latitude, 'longitude': data.longitude})
            coords = dataframe.as_matrix(columns=['latitude', 'longitude'])

            epsilon = geo_fence_distance / (
                    epsilon_constant * km_per_radian)
            db = DBSCAN(eps=epsilon, min_samples=min_points_in_cluster,
                        algorithm='ball_tree', metric='haversine').fit(
                np.radians(coords))
            cluster_labels = db.labels_
            num_clusters = len(set(cluster_labels))
            clusters = pd.Series(
                [coords[cluster_labels == n] for n in range(-1, num_clusters)])
            clusters = clusters.apply(lambda y: np.nan if len(y) == 0 else y)
            clusters.dropna(how='any', inplace=True)
            centermost_points = clusters.map(get_centermost_point)
            centermost_points = np.array(centermost_points)
            all_centroid = []
            for cols in centermost_points:
                cols = np.array(cols)
                cols.flatten()
                cs = ([timestamp, localtime, user, version, cols[latitude], cols[longitude]])
                all_centroid.append(cs)
            df = pd.DataFrame(all_centroid, columns=["timestamp", "localtime", "user", "version", 'latitude', 'longitude'])
            return df
        except:
            pass
    
    # TODO: check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception("DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")
    
    data = ds._data.apply(gps_clusters)
    return DataStream(data=data, metadata=Metadata())

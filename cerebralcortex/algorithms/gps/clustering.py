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

import numpy as np
import pandas as pd
from geopy.distance import great_circle
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.group import GroupedData
from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType
from scipy.spatial import ConvexHull
from shapely.geometry.multipoint import MultiPoint
from sklearn.cluster import DBSCAN

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.algorithms.utils.util import update_metadata
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


def impute_gps_data(ds, accuracy_threashold:int=100):
    """
    Inpute GPS data

    Args:
        ds (DataStream): Windowed/grouped DataStream object
        accuracy_threashold (int):

    Returns:
        DataStream object
    """
    schema = ds._data.schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def gps_imputer(data):
        data = data.sort_values('localtime').reset_index(drop=True)
        data['latitude'][data.accuracy > accuracy_threashold] = np.nan
        data['longitude'][data.accuracy > accuracy_threashold] = np.nan
        data = data.fillna(method='ffill').dropna()
        return data

    # check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception(
            "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")

    data = ds._data.apply(gps_imputer)
    results = DataStream(data=data, metadata=Metadata())
    metadta = update_metadata(stream_metadata=results.metadata,
                              stream_name="gps--org.md2k.imputed",
                              stream_desc="impute GPS data",
                              module_name="cerebralcortex.algorithms.gps.clustering.impute_gps_data",
                              module_version="1.0.0",
                              authors=[{"Azim": "aungkonazim@gmail.com"}])
    results.metadata = metadta
    return results




def cluster_gps(ds: DataStream, epsilon_constant:int = 1000,
                km_per_radian:int = 6371.0088,
                geo_fence_distance:int = 30,
                minimum_points_in_cluster:int = 1,
                latitude_column_name:str = 'latitude',
                longitude_column_name:str = 'longitude'):

    """
    Cluster GPS data - Algorithm used to cluster GPS data is based on DBScan

    Args:
        ds (DataStream): Windowed/grouped DataStream object
        epsilon_constant (int):
        km_per_radian (int):
        geo_fence_distance (int):
        minimum_points_in_cluster (int):
        latitude_column_name (str):
        longitude_column_name (str):

    Returns:
        DataStream object
    """
    centroid_id_name = 'centroid_id'
    features_list = [StructField('centroid_longitude', DoubleType()),
                     StructField('centroid_latitude', DoubleType()),
                     StructField('centroid_id', IntegerType()),
                     StructField('centroid_area', DoubleType())]

    schema = StructType(ds._data._df.schema.fields + features_list)
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
        Get center most point of a cluster

        Args:
            cluster (np.ndarray):

        Returns:

        """
        try:
            if cluster.shape[0]>=3:
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
    @CC_MProvAgg('gps--org.md2k.phonesensor--phone', 'gps_clustering', 'gps--org.md2k.clusters', ['user', 'timestamp'], ['user', 'timestamp'])
    def gps_clustering(data):
        if data.shape[0] < minimum_points_in_cluster:
            return pd.DataFrame([], columns=column_names)
        elif data.shape[0] < 2:
            data['centroid_area'] = 1
            data['centroid_id'] = 0
            data['centroid_latitude'] = data[latitude_column_name].values[0]
            data['centroid_longitude'] = data[longitude_column_name].values[0]
            return data

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

    # check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception(
            "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")

    data = ds._data.apply(gps_clustering)
    results = DataStream(data=data, metadata=Metadata())
    metadta = update_metadata(stream_metadata=results.metadata,
                              stream_name="gps--org.md2k.clusters",
                              stream_desc="GPS clusters computed using DBSCAN algorithm.",
                              module_name="cerebralcortex.algorithms.gps.clustering.cluster_gps",
                              module_version="1.0.0",
                              authors=[{"Azim": "aungkonazim@gmail.com"}])
    results.metadata = metadta
    return results
from cerebralcortex.kernel import Kernel
from cerebralcortex.core.util.spark_helper import get_or_create_sc
import pandas as pd
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates, get_max_features, \
    reorder_columns, classify_brushing
from datetime import datetime
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from pyspark.sql import functions as F

sqlContext = get_or_create_sc(enable_spark_ui=True, type="sqlContext")
ds_candidates = sqlContext.read.parquet(
    "/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/features/user=820c/brushing_candidates/")
##########################################################################################################


CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", study_name="moral")

ds_ag_candidates = DataStream(data=ds_candidates, metadata=Metadata())

## compute features
ds_fouriar_features = ds_ag_candidates.compute_fouriar_features(
    exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"])

ds_statistical_features = ds_ag_candidates.compute_statistical_features(
    exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"],
    feature_names=['mean', 'median', 'stddev', 'skew',
                   'kurt', 'power', 'zero_cross_rate'])

ds_corr_mse_features = ds_ag_candidates.compute_corr_mse_accel_gyro(
    exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"])

ds_features = ds_fouriar_features \
    .join(ds_statistical_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'],
          how='full') \
    .join(ds_corr_mse_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'], how='full')

ds_features = ds_features.withColumn("duration",
                                     (ds_features.end_time.cast("long") - ds_features.start_time.cast("long")))

ds_features = get_max_features(ds_features)

ds_features = reorder_columns(ds_features)

ds_features._data.repartition(1).write.mode("overwrite").parquet(
    "/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/features/user=820c/brushing_features")

print("END-TIME", datetime.now())

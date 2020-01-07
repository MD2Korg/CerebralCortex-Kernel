from cerebralcortex.kernel import Kernel
import pandas as pd
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates, get_max_features, \
    reorder_columns, classify_brushing
from datetime import datetime
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from pyspark.sql import functions as F
from cerebralcortex.kernel import Kernel
import pandas as pd
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates, filter_candidates
from datetime import datetime
from pyspark.sql.functions import pandas_udf,PandasUDFType

###!!!                 CC OBJECT CREATION & data config           !!!###
CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", study_name="moral")

wrist = "right"

accel_stream_name = "accelerometer--org.md2k.motionsense--motion_sense--"+wrist+"_wrist"
gyro_stream_name = "gyroscope--org.md2k.motionsense--motion_sense--"+wrist+"_wrist"

user_id = "820c_03_18_2017"

###!!!                 GET DATA RAW DATA                          !!!###
ds_accel = CC.get_stream(accel_stream_name, user_id=user_id)
ds_gyro = CC.get_stream(gyro_stream_name, user_id=user_id)

###!!!                 BRUSHING CANDIDATE GENERATION              !!!###
# interpolation
ds_accel_interpolated = ds_accel.interpolate(limit=1)
ds_gyro_interpolated = ds_gyro.interpolate()

##compute magnitude
ds_accel_magnitude = ds_accel_interpolated.compute_magnitude(
    col_names=["accelerometer_x", "accelerometer_y", "accelerometer_z"], magnitude_col_name="accel_magnitude")
ds_gyro_magnitude = ds_gyro_interpolated.compute_magnitude(col_names=["gyroscope_x", "gyroscope_y", "gyroscope_z"],
                                                           magnitude_col_name="gyro_magnitude")

# join accel and gyro streams
ds_ag = ds_accel_magnitude.join(ds_gyro_magnitude, on=['user', 'timestamp', 'localtime', 'version'],
                                how='full').dropna()

# get orientation
ds_ag_orientation = get_orientation_data(ds_ag, wrist="left")

## apply complementary filter
ds_ag_complemtary_filtered = ds_ag_orientation.complementary_filter()

# get brushing candidate groups
ds_ag_candidates = get_candidates(ds_ag_complemtary_filtered)

# remove where group==0 - non-candidates

ds_ag_candidates = filter_candidates(ds_ag_candidates)

## set metadata details for candidate stream
candidate_stream_name = "brushing-candidates--org.md2k.motionsense--motion_sense--"+wrist+"_wrist"
ds_ag_candidates.metadata.name = candidate_stream_name
ds_ag_candidates.metadata.description = "This stream contains the 'Brushing Candidates Based on Event'. group column contains the ids of each candidate group and candidate column represents whether a datapoint is a candidate or not."

CC.save_stream(ds_ag_candidates)

print("Generate candidates stream.")

###!!!                 CC OBJECT CREATION                         !!!###

ds_candidates = CC.get_stream(candidate_stream_name, user_id=user_id)

## compute features
ds_fouriar_features = ds_candidates.compute_fouriar_features(
    exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"])

ds_statistical_features = ds_candidates.compute_statistical_features(
    exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"],
    feature_names=['mean', 'median', 'stddev', 'skew',
                   'kurt', 'power', 'zero_cross_rate'])

ds_corr_mse_features = ds_candidates.compute_corr_mse_accel_gyro(
    exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"])

ds_features = ds_fouriar_features \
    .join(ds_statistical_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'],
          how='full') \
    .join(ds_corr_mse_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'], how='full')

ds_features = ds_features.withColumn("duration",
                                     (ds_features.end_time.cast("long") - ds_features.start_time.cast("long")))

ds_features = get_max_features(ds_features)

ds_features = reorder_columns(ds_features)

## set metadata details for candidate stream
features_stream_name = "brushing-features--org.md2k.motionsense--motion_sense--"+wrist+"_wrist"
ds_features.metadata.name = features_stream_name
ds_features.metadata.description = "This stream contains the brushing features computed for each brushing candidate window."

CC.save_stream(ds_features)

###!!!                 CC OBJECT CREATION                         !!!###

from cerebralcortex.kernel import Kernel
import pandas as pd
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates, filter_candidates
from datetime import datetime

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", study_name="moral")

ds_accel = CC.get_stream("accelerometer--org.md2k.motionsense--motion_sense--left_wrist", user_id="820c_03_18_2017")
ds_gyro = CC.get_stream("gyroscope--org.md2k.motionsense--motion_sense--left_wrist", user_id="820c_03_18_2017")

print("START-TIME", datetime.now())
# interpolation
ds_accel_interpolated = ds_accel.interpolate()
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

ds_ag_candidates._data.repartition(1).write.mode("overwrite").parquet(
    "/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/features/user=820c/brushing_candidates")
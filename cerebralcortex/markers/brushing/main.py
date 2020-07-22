import argparse

from cerebralcortex.kernel import Kernel
from cerebralcortex.markers.brushing.util import get_max_features, reorder_columns, classify_brushing
from cerebralcortex.markers.brushing.util import get_orientation_data, get_candidates, filter_candidates


def generate_candidates(CC, user_id, accel_stream_name, gyro_stream_name, output_stream_name):
    ds_accel = CC.get_stream(accel_stream_name, user_id=user_id)
    ds_gyro = CC.get_stream(gyro_stream_name, user_id=user_id)

    # interpolation - default interpolation is linear
    ds_accel_interpolated = ds_accel.interpolate()
    ds_gyro_interpolated = ds_gyro.interpolate()

    ##compute magnitude - xyz axis of accell and gyro
    ds_accel_magnitude = ds_accel_interpolated.compute_magnitude(
        col_names=["accelerometer_x", "accelerometer_y", "accelerometer_z"], magnitude_col_name="accel_magnitude")
    ds_gyro_magnitude = ds_gyro_interpolated.compute_magnitude(col_names=["gyroscope_x", "gyroscope_y", "gyroscope_z"],
                                                               magnitude_col_name="gyro_magnitude")

    # join accel and gyro streams
    ds_ag = ds_accel_magnitude.join(ds_gyro_magnitude, on=['user', 'timestamp', 'localtime', 'version'],
                                    how='full').dropna()

    # get wrist orientation
    ds_ag_orientation = get_orientation_data(ds_ag, wrist="left")

    ## apply complementary filter - roll, pitch, yaw
    ds_ag_complemtary_filtered = ds_ag_orientation.complementary_filter()

    # get brushing candidate groups
    ds_ag_candidates = get_candidates(ds_ag_complemtary_filtered)

    # remove where group==0 - non-candidates
    ds_ag_candidates = filter_candidates(ds_ag_candidates)

    ## set metadata details for candidate stream
    ds_ag_candidates.metadata.name = output_stream_name
    ds_ag_candidates.metadata.description = "This stream contains the 'Brushing Candidates Based on Event'. group column contains the ids of each candidate group and candidate column represents whether a datapoint is a candidate or not."

    CC.save_stream(ds_ag_candidates, overwrite=True)

    print("Generated candidates stream.")

def generate_features(CC, user_id, candidate_stream_name, output_stream_name):
    ds_candidates = CC.get_stream(candidate_stream_name, user_id=user_id)

    ## compute features
    ds_fouriar_features = ds_candidates.compute_FFT_features(
        exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"])

    ds_statistical_features = ds_candidates.compute_statistical_features(
        exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"],
        feature_names=['mean', 'median', 'stddev', 'skew',
                       'kurt', 'sqr', 'zero_cross_rate'])

    ds_corr_mse_features = ds_candidates.compute_corr_mse_accel_gyro(
        exclude_col_names=['group', 'candidate', "accel_magnitude", "gyro_magnitude"], groupByColumnName=["group"])

    ds_features = ds_fouriar_features \
        .join(ds_statistical_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'],
              how='full') \
        .join(ds_corr_mse_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'],
              how='full')

    ds_features = ds_features.withColumn("duration",
                                         (ds_features.end_time.cast("long") - ds_features.start_time.cast("long")))

    ds_features = get_max_features(ds_features)

    ## set metadata details for candidate stream
    ds_features.metadata.name = output_stream_name
    ds_features.metadata.description = "This stream contains the brushing features computed for each brushing candidate window."

    CC.save_stream(ds_features, overwrite=True)

    print("Generated brushing features.")

def predict_brushing(CC, user_id, features_stream_name):
    features = CC.get_stream(features_stream_name, user_id=user_id)

    features = reorder_columns(features)
    pdf_features = features.toPandas()
    pdf_predictions = classify_brushing(pdf_features, model_file_name="model/AB_model_brushing_all_features.model")
    pdf_features['predictions'] = pdf_predictions
    detected_episodes = pdf_features.loc[pdf_features['predictions'] == 1]

    print("PREDICTED BRUSHING ESPISODES\n")
    print(detected_episodes[["start_time", "end_time"]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Brushing episode detection")

    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=True)
    parser.add_argument('-a', '--accel_stream_name', help='Accel stream name', required=True)
    parser.add_argument('-g', '--gyro_stream_name', help='Gyro stream name', required=True)
    parser.add_argument('-w', '--wrist', help="wrist ('left', 'right')", required=True)
    parser.add_argument('-u', '--user_id', help='User ID. Optional if you want to process data for just one user',
                        required=True)

    args = vars(parser.parse_args())

    config_dir = str(args["config_dir"]).strip()
    accel_stream_name = str(args["accel_stream_name"]).strip()
    gyro_stream_name = str(args["gyro_stream_name"]).strip()
    wrist = str(args["wrist"]).strip()
    user_id = str(args["user_id"]).strip()

    CC = Kernel(config_dir, study_name="moral")

    candidate_stream_name = "brushing-candidates--org.md2k.motionsense--motion_sense--" + wrist + "_wrist"
    features_stream_name = "brushing-features--org.md2k.motionsense--motion_sense--" + wrist + "_wrist"

    generate_candidates(CC, user_id=user_id, accel_stream_name=accel_stream_name, gyro_stream_name=gyro_stream_name,
                        output_stream_name=candidate_stream_name)

    generate_features(CC, user_id=user_id, candidate_stream_name=candidate_stream_name,
                      output_stream_name=features_stream_name)

    predict_brushing(CC, user_id=user_id, features_stream_name=features_stream_name)

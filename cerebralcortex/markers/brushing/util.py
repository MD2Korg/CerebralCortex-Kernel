import pickle

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from pyspark.sql.types import StructType
from pyspark.sql.window import Window


#re-orient-signal
def get_orientation_data(ds, wrist, ori=1, is_new_device=False,
                         accelerometer_x="accelerometer_x", accelerometer_y="accelerometer_y",
                         accelerometer_z="accelerometer_z",
                         gyroscope_x="gyroscope_x", gyroscope_y="gyroscope_y", gyroscope_z="gyroscope_z"):
    """
    Get the orientation of hand using accel and gyro data. 
    Args:
        ds: DataStream object
        wrist: name of the wrist smart watch was worn
        ori: 
        is_new_device: this param is for motionsense smart watch version 
        accelerometer_x (float): 
        accelerometer_y (float): 
        accelerometer_z (float): 
        gyroscope_x (float): 
        gyroscope_y (float): 
        gyroscope_z (float): 

    Returns:
        DataStream object
        
    """
    left_ori = {"old": {0: [1, 1, 1], 1: [1, 1, 1], 2: [-1, -1, 1], 3: [-1, 1, 1], 4: [1, -1, 1]},
                "new": {0: [-1, 1, 1], 1: [-1, 1, 1], 2: [1, -1, 1], 3: [1, 1, 1], 4: [-1, -1, 1]}}
    right_ori = {"old": {0: [1, -1, 1], 1: [1, -1, 1], 2: [-1, 1, 1], 3: [-1, -1, 1], 4: [1, 1, 1]},
                 "new": {0: [1, 1, 1], 1: [1, 1, 1], 2: [-1, -1, 1], 3: [-1, 1, 1], 4: [1, -1, 1]}}
    if is_new_device:
        left_fac = left_ori.get("new").get(ori)
        right_fac = right_ori.get("new").get(ori)

    else:
        left_fac = left_ori.get("old").get(ori)
        right_fac = right_ori.get("old").get(ori)

    if wrist == "left":
        fac = left_fac
    elif wrist == "right":
        fac = right_fac
    else:
        raise Exception("wrist can only be left or right.")

    data = ds.withColumn(gyroscope_x, ds[gyroscope_x] * fac[0]) \
        .withColumn(gyroscope_y, ds[gyroscope_y] * fac[1]) \
        .withColumn(gyroscope_z, ds[gyroscope_z] * fac[2]) \
        .withColumn(accelerometer_x, ds[accelerometer_x] * fac[0]) \
        .withColumn(accelerometer_y, ds[accelerometer_y] * fac[1]) \
        .withColumn(accelerometer_z, ds[accelerometer_z] * fac[2])

    return data


def get_candidates(ds, uper_limit: float = 0.1, threshold: float = 0.5):
    """
    Get brushing candidates. Data is windowed into potential brushing candidate
    Args:
        ds (DataStream):
        uper_limit (float): threashold for accel. This is used to know how high the hand is
        threshold (float): 

    Returns:

    """
    window = Window.partitionBy(["user", "version"]).rowsBetween(-3, 3).orderBy("timestamp")
    window2 = Window.orderBy("timestamp")

    df1 = ds.withColumn("candidate", F.when(F.col("accelerometer_y") > uper_limit, F.lit(1)).otherwise(F.lit(0)))

    df = df1.withColumn("candidate",
                        F.when((F.avg(df1.candidate).over(window)) >= threshold, F.lit(1))
                        .otherwise(F.lit(0)))

    df2 = df.withColumn(
        "userChange",
        (F.col("user") != F.lag("user").over(window2)).cast("int")
    ) \
        .withColumn(
        "candidateChange",
        (F.col("candidate") != F.lag("candidate").over(window2)).cast("int")
    ) \
        .fillna(
        0,
        subset=["userChange", "candidateChange"]
    ) \
        .withColumn(
        "indicator",
        (~((F.col("userChange") == 0) & (F.col("candidateChange") == 0))).cast("int")
    ) \
        .withColumn(
        "group",
        F.sum(F.col("indicator")).over(window2.rangeBetween(Window.unboundedPreceding, 0))
    ).drop("userChange").drop("candidateChange").drop("indicator")

    return df2


def get_max_features(ds):
    """
    This method will compute what are the max values for accel and gyro statistical/FFT features
    Args:
        ds (DataStream): 

    Returns:
        DataStream
    """
    basic_schema = ds.schema
    max_feature_schema = [
        StructField("max_accl_mean", FloatType()),
        StructField("max_accl_median", FloatType()),
        StructField("max_accl_stddev", FloatType()),
        StructField("max_accl_skew", FloatType()),
        StructField("max_accl_kurt", FloatType()),
        StructField("max_accl_sqr", FloatType()),
        StructField("max_accl_zero_cross_rate", FloatType()),
        StructField("max_accl_fft_centroid", FloatType()),
        StructField("max_accl_fft_spread", FloatType()),
        StructField("max_accl_spectral_entropy", FloatType()),
        StructField("max_accl_spectral_entropy_old", FloatType()),
        StructField("max_accl_fft_flux", FloatType()),
        StructField("max_accl_spectral_folloff", FloatType())
    ]

    features_schema = StructType(basic_schema.fields + max_feature_schema)

    @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
    def get_max_vals_features(df):
        vals = []
        max_accl_mean, max_accl_median, max_accl_stddev, max_accl_skew, max_accl_kurt, max_accl_sqr, max_accl_zero_cross_rate, max_accl_fft_centroid, max_accl_fft_spread, max_accl_spectral_entropy, max_accl_spectral_entropy_old, max_accl_fft_flux, max_accl_spectral_folloff = (
        [] for i in range(13))
        vals.append(df['timestamp'].iloc[0])
        vals.append(df['localtime'].iloc[0])
        vals.append(df['user'].iloc[0])
        vals.append(df['version'].iloc[0])
        vals.append(df['timestamp'].iloc[0])
        vals.append(df['timestamp'].iloc[-1])

        for indx, row in df.iterrows():
            max_accl_mean.append(
                max(row["accelerometer_x_mean"], row["accelerometer_y_mean"], row["accelerometer_z_mean"]))
            max_accl_median.append(
                max(row["accelerometer_x_median"], row["accelerometer_y_median"], row["accelerometer_z_median"]))
            max_accl_stddev.append(
                max(row["accelerometer_x_stddev"], row["accelerometer_y_stddev"], row["accelerometer_z_stddev"]))
            max_accl_skew.append(
                max(row["accelerometer_x_skew"], row["accelerometer_y_skew"], row["accelerometer_z_skew"]))
            max_accl_kurt.append(
                max(row["accelerometer_x_kurt"], row["accelerometer_y_kurt"], row["accelerometer_z_kurt"]))
            max_accl_sqr.append(
                max(row["accelerometer_x_sqr"], row["accelerometer_y_sqr"], row["accelerometer_z_sqr"]))
            max_accl_zero_cross_rate.append(
                max(row["accelerometer_x_zero_cross_rate"], row["accelerometer_y_zero_cross_rate"],
                    row["accelerometer_z_zero_cross_rate"]))
            max_accl_fft_centroid.append(max(row["accelerometer_x_fft_centroid"], row["accelerometer_y_fft_centroid"],
                                             row["accelerometer_z_fft_centroid"]))
            max_accl_fft_spread.append(max(row["accelerometer_x_fft_spread"], row["accelerometer_y_fft_spread"],
                                           row["accelerometer_z_fft_spread"]))
            max_accl_spectral_entropy.append(
                max(row["accelerometer_x_spectral_entropy"], row["accelerometer_y_spectral_entropy"],
                    row["accelerometer_z_spectral_entropy"]))
            max_accl_spectral_entropy_old.append(
                max(row["accelerometer_x_spectral_entropy_old"], row["accelerometer_y_spectral_entropy_old"],
                    row["accelerometer_z_spectral_entropy_old"]))
            max_accl_fft_flux.append(
                max(row["accelerometer_x_fft_flux"], row["accelerometer_y_fft_flux"], row["accelerometer_z_fft_flux"]))
            max_accl_spectral_folloff.append(
                max(row["accelerometer_x_spectral_folloff"], row["accelerometer_y_spectral_folloff"],
                    row["accelerometer_z_spectral_folloff"]))

        df["max_accl_mean"] = max_accl_mean
        df["max_accl_median"] = max_accl_median
        df["max_accl_stddev"] = max_accl_stddev
        df["max_accl_skew"] = max_accl_skew
        df["max_accl_kurt"] = max_accl_kurt
        df["max_accl_sqr"] = max_accl_sqr
        df["max_accl_zero_cross_rate"] = max_accl_zero_cross_rate
        df["max_accl_fft_centroid"] = max_accl_fft_centroid
        df["max_accl_fft_spread"] = max_accl_fft_spread
        df["max_accl_spectral_entropy"] = max_accl_spectral_entropy
        df["max_accl_spectral_entropy_old"] = max_accl_spectral_entropy_old
        df["max_accl_fft_flux"] = max_accl_fft_flux
        df["max_accl_spectral_folloff"] = max_accl_spectral_folloff

        return df

    return ds.compute(get_max_vals_features)


def filter_candidates(ds):
    features_schema = ds.schema
    MIN_SEGMENT_DURATION = 15  # seconds
    MAX_SEGMENT_DURATION = 5 * 60  # seconds

    @pandas_udf(features_schema, PandasUDFType.GROUPED_MAP)
    def get_max_vals_features(df):
        if df['candidate'].iloc[0] == 1:
            duration = (df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).seconds
            if duration >= MIN_SEGMENT_DURATION and duration <= MAX_SEGMENT_DURATION:
                return df
            else:
                return pd.DataFrame(columns=df.columns)
        else:
            return pd.DataFrame(columns=df.columns)

    return ds.compute(get_max_vals_features, groupByColumnName=["group"])


def reorder_columns(ds):
    feature_names = ['accelerometer_x', 'accelerometer_y', 'accelerometer_z', 'max_accl', 'gyroscope_y', 'gyroscope_x',
                     'gyroscope_z', 'roll', 'pitch', 'yaw']
    sensor_names = ['mean', 'median', 'stddev', 'skew', 'kurt', 'sqr', 'zero_cross_rate', "fft_centroid",
                    'fft_spread', 'spectral_entropy', 'spectral_entropy_old', 'fft_flux', 'spectral_folloff']
    extra_features = ["ax_ay_corr", 'ax_az_corr', 'ay_az_corr', 'gx_gy_corr', 'gx_gz_corr', 'gy_gz_corr', 'ax_ay_mse',
                      'ax_az_mse', 'ay_az_mse', 'gx_gy_mse', 'gx_gz_mse', 'gy_gz_mse']
    col_names = ["timestamp", "localtime", "user", "version", "start_time", "end_time", "duration"]

    for fn in feature_names:
        for sn in sensor_names:
            col_names.append(fn + "_" + sn)
    col_names.extend(extra_features)
    ds = ds.select(*col_names)
    ds = ds.orderBy("timestamp")
    return ds


def classify_brushing(X: pd.DataFrame, model_file_name: str):
    with open(model_file_name, 'rb') as handle:
        clf = pickle.load(handle)
    X = X.values
    X = X[:, 6:]
    preds = clf.predict(X)

    return preds

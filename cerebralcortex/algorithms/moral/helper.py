def get_orientation_data(ds, sensor_type, wrist, ori=1, is_new_device=False):
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

    if sensor_type == "gyro":
        data = ds.withColumn("accelerometer_x", ds.accelerometer_x * fac[0]) \
            .withColumn("accelerometer_y", ds.accelerometer_y * fac[1]) \
            .withColumn("accelerometer_z", ds.accelerometer_z * fac[2])
    elif sensor_type == "accel":
        data = ds.withColumn("gyroscope_x", ds.accelerometer_x * fac[0]) \
            .withColumn("gyroscope_y", ds.accelerometer_y * fac[1]) \
            .withColumn("gyroscope_z", ds.accelerometer_z * fac[2])
    else:
        raise Exception("Only gyro or accel sensor_type are allowed.")

    return data

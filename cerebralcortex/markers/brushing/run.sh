#!/usr/bin/env bash

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

# export CerebralCortex path if CerebralCortex is not installed
export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0/"

# Update path to libhdfs.so if it's different than /usr/local/hadoop/lib/native/libhdfs.so
# uncooment it if using HDFS as NoSQl storage
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/hadoop/lib/native/libhdfs.so

#Spark path, uncomment if spark home is not exported else where.
#export SPARK_HOME=/home/ali/spark/spark-3.0.0-preview2-bin-hadoop2.7/

#set spark home, uncomment if spark home is not exported else where.
export PATH=$SPARK_HOME/bin:$PATH



#########################################################################################
############################ YAML Config Paths and other configs ########################
#########################################################################################

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/home/ali/IdeaProjects/CerebralCortex-2.0/conf/"

# Brushing detection algorithm's input params
ACCEL_STREAM_NAME="accelerometer--org.md2k.motionsense--motion_sense--right_wrist"
GYRO_STREAM_NAME="gyroscope--org.md2k.motionsense--motion_sense--right_wrist"
WRIST="right"
USER_ID="820c_03_18_2017"

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="local[3]"
SPARK_UI_PORT=4087


# add -u $USER_ID at the end of below command if user_id is provided above
spark-submit --master $SPARK_MASTER --total-executor-cores 1 --driver-memory 16g --executor-memory 2g main.py -c $CONFIG_DIRECTORY -a $ACCEL_STREAM_NAME -g $GYRO_STREAM_NAME -w $WRIST -u $USER_ID
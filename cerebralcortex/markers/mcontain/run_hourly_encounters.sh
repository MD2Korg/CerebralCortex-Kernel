#!/usr/bin/env bash

export PYTHONPATH="${PYTHONPATH}:/home/cnali/code/CerebralCortex-Kernel/"

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

# export CerebralCortex path if CerebralCortex is not installed
#export PYTHONPATH="${PYTHONPATH}:/cerebralcortex/code/ali/CerebralCortex/"

# Update path to libhdfs.so if it's different than /usr/local/hadoop/lib/native/libhdfs.so
# uncooment it if using HDFS as NoSQl storage
export LD_LIBRARY_PATH="/usr/local/hadoop/lib/native/libhdfs.so"

#Spark path, uncomment if spark home is not exported else where.
#export SPARK_HOME=/home/ali/spark/spark-2.2.1-bin-hadoop2.7/

#set spark home, uncomment if spark home is not exported else where.
#export PATH=$SPARK_HOME/bin:$PATH



#########################################################################################
############################ YAML Config Paths and other configs ########################
#########################################################################################

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/cerebralcortex/code/config/cc3_conf/"
INPUT_STREAM_NAME="beacon--org.md2k.mcontain--phone"
INPUT_MAP_STREAM_NAME="mcontain_user_mapping"
START_TIME='2020-03-31 12:00' #date -d '1 hour ago' "+%Y-%m-%d %H:%M:%S"
END_TIME='2020-03-31 13:00' #date "+%Y-%m-%d %H:%M:%S"
LTIME=1

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="local[30]"
SPARK_UI_PORT=4087

PY_FILES="/home/cnali/code/CerebralCortex-Kernel/dist/cerebralcortex_kernel-3.1.1.post3-py3.6.egg"

spark-submit --master $SPARK_MASTER --conf spark.ui.port=$SPARK_UI_PORT --total-executor-cores 1 --driver-memory 1g --executor-memory 1g --py-files $PY_FILES hourly_encounters.py -c $CONFIG_DIRECTORY -a $INPUT_STREAM_NAME -b $INPUT_MAP_STREAM_NAME -s "$START_TIME" -e "$END_TIME" -l $LTIME
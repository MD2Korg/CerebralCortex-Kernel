#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# add current project root directory to pythonpath
export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0/"

#Spark path
export SPARK_HOME=/home/ali/spark/spark-2.2.0-bin-hadoop2.7/

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/core/resources/cc_configuration.yml"
# data directory where all gz and json files are stored
DATA_DIR="/home/ali/IdeaProjects/MD2K_DATA/17446/"
# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="5"
# kafka broker ip with port, more than one brokers shale be separated by command
KAFKA_BROKER="127.0.0.1:9092"
# spark master
SPARK_MASTER="local[*]"

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -b $KAFKA_BROKER -bd $BATCH_DURATION -spm $SPARK_MASTER
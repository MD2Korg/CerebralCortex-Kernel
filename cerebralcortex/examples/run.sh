#!/usr/bin/env bash

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# export CerebralCortex path if NOT INSTALLED
# export PYTHONPATH="${PYTHONPATH}:PATH-OF-MAIN-DIR/CerebralCortex-Kernel-3.0
#export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0"

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="local[*]"

# add -p $PARTICIPANTS at the end of below command if participants' UUIDs are provided
spark-submit main.py
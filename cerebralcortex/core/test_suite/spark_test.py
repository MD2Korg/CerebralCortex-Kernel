# THIS TMP FILE IS FOR AWS-S3

import time
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc

cc_config = "/home/hadoop/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"
CC = CerebralCortex(cc_config)
spark_context = get_or_create_sc(type="sparkContext")

study_name = "mperf"
all_users = CC.get_all_users(study_name)

if all_users:
    rdd = spark_context.parallelize(all_users)
    results = rdd.map(
        lambda user: example_method(user["identifier"], cc_config))
    results.count()
else:
    print(study_name, "- study has 0 users.")


def example_method(user_id, cc_config):
    CC = CerebralCortex(cc_config)
    user_name = CC.get_user_name(user_id)
    print("User ID:", user_id, "User Name:", user_name)
    time.sleep(10)
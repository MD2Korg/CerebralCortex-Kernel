# THIS TMP FILE IS FOR AWS-S3

import time
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc
import uuid

cc_config = "/home/hadoop/cc_aws_config.yml"
CC = CerebralCortex(cc_config)
spark_context = get_or_create_sc(type="sparkContext")

def example_method(user_id, cc_config):
    CC = CerebralCortex(cc_config)
    user_name = CC.get_user_name(user_id)
    print("User ID:", user_id, "User Name:", user_name)
    time.sleep(1)

def run_spark_job():
    study_name = "mperf"
    all_users = CC.get_all_users(study_name)

    if all_users:
        rdd = spark_context.parallelize(all_users)
        rdd = rdd.repartition(200)
        results = rdd.map(
            lambda user: example_method(user["identifier"], cc_config))
        results.count()
    else:
        print(study_name, "- study has 0 users.")

def aws_s3_read_write():
    ds = CC.get_stream("21bc8ead-130c-3e16-8054-3f02c5343f86","00ab666c-afb8-476e-9872-6472b4e66b68","20180123")
    print(ds.data[:5])

    ds._identifier = str(uuid.UUID('{00000000-5087-3d56-ad0e-0b27c3c83182}'))
    ds._owner = str(uuid.UUID('{00000000-f81c-44d2-9db8-fea69f468d58}'))
    CC.save_stream(ds)

# test spark
#run_spark_job()

# test read/write aws-s3
aws_s3_read_write()


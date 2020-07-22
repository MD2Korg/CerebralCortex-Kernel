import argparse

from pyspark.sql import functions as F

from cerebralcortex.kernel import Kernel


def assign_covid_user(data,covid_users):
    if not isinstance(covid_users,list):
        covid_users = [covid_users]
    data = data.withColumn('covid',F.when(F.col('user').isin(covid_users), 1 ).when(F.col('participant_identifier').isin(covid_users), 2).otherwise(0))
    return data

def make_CC_object(config_dir="/home/jupyter/cc3_conf/",
                   study_name='mcontain'):
    CC = Kernel(config_dir, study_name=study_name)
    return CC

def save_data(CC,data_result,centroid_present=True,metadata=None):
    if centroid_present:
        columns = ['centroid_id',
                   'centroid_latitude',
                   'centroid_longitude',
                   'centroid_area']
        for c in columns:
            if c in data_result.columns:
                data_result = data_result.drop(*[c])
    data_result.metadata = metadata
    CC.save_stream(data_result,overwrite=False)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assign users as Covid-19 affected")

    parser.add_argument('-c', '--users', help='list of user ids', required=True)
    parser.add_argument('-c', '--input_stream_name', help='Encounter stream name', required=True)
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=True)

    args = vars(parser.parse_args())
    users = args["users"]
    config_dir = str(args["config_dir"]).strip()
    input_stream_name = str(args["input_stream_name"]).strip()
    CC = make_CC_object(config_dir)
    data = CC.get_stream(input_stream_name)
    metadata = data.metadata
    data = assign_covid_user(data,users)
    save_data(CC,data,centroid_present=False,metadata=metadata)

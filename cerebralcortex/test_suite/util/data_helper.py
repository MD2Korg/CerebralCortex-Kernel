from datetime import datetime, timedelta
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
def gen_phone_battery_data():
    column_name = ["timestamp", "offset", "battery_level", "ver", "user"]
    sample_data = []
    timestamp = datetime(2019,1,9,11,34,59)
    tmp = 1
    sample = 100
    sqlContext = get_or_create_sc("sqlContext")
    for row in range(1000, 1, -1):
        tmp +=1
        if tmp==100:
            sample = sample-1
            tmp = 1
        timestamp = timestamp+timedelta(0,1)
        sample_data.append((timestamp, "21600000", sample, 1, "00000000-afb8-476e-9872-6472b4e66b68"))
    df = sqlContext.createDataFrame(sample_data, column_name)
    return df

def gen_phone_battery_metadata():
    stream_metadata = Metadata()
    stream_metadata\
        .add_dataDescriptor(DataDescriptor().name("level").type("float").set_attirubte("description", "current battery charge"))\
        .add_module(ModuleMetadata().module_name("battery").version("1.2.4").set_author("test_user", "test_user@test_email.com"))
    stream_metadata.is_valid()
    return stream_metadata

def create_all(CC):
    # create temp user in SQL storage
    # create temp folders for test-data
    pass

def drop_all(CC):
    # delete temp user in SQL storage
    # delete temp folders for test-data
    pass

gen_phone_battery_data()
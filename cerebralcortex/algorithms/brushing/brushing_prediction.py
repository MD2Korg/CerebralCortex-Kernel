from cerebralcortex.kernel import Kernel
from cerebralcortex.core.util.spark_helper import get_or_create_sc
import pandas as pd
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates, get_max_features, \
    reorder_columns, classify_brushing
from datetime import datetime
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from pyspark.sql import functions as F

sqlContext = get_or_create_sc(enable_spark_ui=True, type="sqlContext")
df_features = sqlContext.read.parquet(
    "/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/features/user=820c/brushing_features/")
##########################################################################################################
pd.set_option('display.max_colwidth', -1)

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", study_name="moral")

ds_features = DataStream(data=df_features, metadata=Metadata())

pdf_features = ds_features.toPandas()

pdf_predictions = classify_brushing(pdf_features,model_file_name="/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/algorithms/brushing/model/AB_model_brushing_all_features.model")

pdf_features['predictions'] = pdf_predictions

detected_episodes = pdf_features.loc[pdf_features['predictions']==1]

print(detected_episodes['start_time'], detected_episodes['end_time'])

print(pdf_predictions)

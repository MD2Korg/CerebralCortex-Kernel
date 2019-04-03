from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import numpy as np
import pandas as pd


schema = StructType([
    StructField("user", StringType()),
    StructField("ecg", FloatType()),
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def process_ecg(data: object) -> object:
    print(len(data))
    print((data.dtypes))
    print(':'*100)
    df = pd.DataFrame([], columns=['user', 'ecg'])
    return df

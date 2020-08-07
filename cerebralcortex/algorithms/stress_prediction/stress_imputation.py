# Copyright (c) 2020, MD2K Center of Excellence
# All rights reserved.
# Md Azim Ullah (mullah@memphis.edu)
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import warnings

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, IntegerType
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata


def get_metadata(stress_imputed_data, output_stream_name, input_stream_name):
    """
    generate metadata for a datastream.

    Args:
        stress_imputed_data (DataStream):
        output_stream_name (str):

    Returns:

    """
    schema = stress_imputed_data.schema
    stream_metadata = Metadata()
    stream_metadata.set_name(output_stream_name).set_description("stress imputed")\
        .add_input_stream(input_stream_name)
    for field in schema.fields:
        stream_metadata.add_dataDescriptor(
            DataDescriptor().set_name(str(field.name)).set_type(str(field.dataType))
        )
    stream_metadata.add_module(
        ModuleMetadata().set_name("stress forward fill imputer") \
            .set_attribute("url", "hhtps://md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    return stream_metadata

def forward_fill_data(stress_data,output_stream_name = 'org.md2k.autosense.ecg.stress.probability.forward.filled',minimum_points_per_day=60):
    """


    Args:
        stress_data (DataStream):
        output_stream_name (str):
        minimum_points_per_day (int):

    Returns:

    """
    if 'stress_probability' not in stress_data.columns and 'stress_likelihood' in stress_data.columns:
        stress_data = stress_data.withColumnRenamed('stress_likelihood','stress_probability')
    stress_data = stress_data.withColumn('day',F.date_format('localtime',"yyyyMMdd"))
    stress_data = stress_data.withColumn('start',F.col('window').start)
    stress_data = stress_data.withColumn('end',F.col('window').end).drop(*['window'])
    stress_data = stress_data.withColumn('start',F.col('start').cast('double'))
    stress_data = stress_data.withColumn('end',F.col('end').cast('double'))
    stress_data = stress_data.withColumn('hour',F.hour('localtime'))
    stress_data = stress_data.withColumn('weekday',F.date_format('localtime','EEEE'))
    stress_data = stress_data.withColumn('localtime',F.col('localtime').cast('double'))
    stress_data = stress_data.withColumn('timestamp',F.col('timestamp').cast('double'))
    schema = StructType([
        StructField("timestamp", DoubleType()),
        StructField("start", DoubleType()),
        StructField("end", DoubleType()),
        StructField("localtime", DoubleType()),
        StructField("version", IntegerType()),
        StructField("user", StringType()),
        StructField("day", StringType()),
        StructField("weekday", StringType()),
        StructField("hour", IntegerType()),
        StructField("stress_probability", DoubleType()),
        StructField("imputed", IntegerType())
    ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('org.md2k.autosense.ecg.stress.probability', 'forward_fill_data', output_stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def impute_forwardfill(data):
        """


        Args:
            data:

        Returns:

        """
        if data.shape[0]<minimum_points_per_day:
            return pd.DataFrame([],columns=['timestamp','localtime','start','end',
                                            'version','user','day','stress_probability',
                                            'weekday','hour','imputed'])
        data['stress_probability'] = data['stress_probability'].rolling(window=5).mean()
        data = data.dropna().sort_values('start').reset_index(drop=True)
        start = data['start'][0]
        all_rows = []
        for i,row in data.iterrows():
            if row['start']==start:
                all_rows.append([row['timestamp'],row['localtime'],row['start'],row['end'],
                                 row['version'],row['user'],row['day'],row['stress_probability'],
                                 row['weekday'],row['hour'],0])
                start = row['end']
            else:
                k = 1
                while (start+k*60)<=row['start']:
                    all_rows.append([data.loc[i-1]['timestamp']+k*60,data.loc[i-1]['localtime']+k*60,
                                     data.loc[i-1]['start']+k*60,data.loc[i-1]['end']+k*60,
                                     row['version'],row['user'],row['day'],
                                     data.loc[i-1]['stress_probability'],data.loc[i-1]['weekday'],
                                     data.loc[i-1]['hour'],1])
                    k+=1
                all_rows.append([row['timestamp'],row['localtime'],row['start'],row['end'],
                                 row['version'],row['user'],row['day'],row['stress_probability'],
                                 row['weekday'],row['hour'],0])
                start = row['end']
        return pd.DataFrame(all_rows,columns=['timestamp','localtime','start','end',
                                              'version','user','day','stress_probability',
                                              'weekday','hour','imputed'])

    stress_imputed_data = stress_data.groupBy(['user','day']).apply(impute_forwardfill)
    stress_imputed_data = stress_imputed_data.withColumn('start',F.col('start').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('end',F.col('end').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('localtime',F.col('localtime').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('timestamp',F.col('timestamp').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('hour',F.hour('localtime'))
    stress_imputed_data = stress_imputed_data.withColumn('weekday',F.date_format('localtime','EEEE'))
    cols = list(stress_imputed_data.columns)
    cols.append(F.struct('start', 'end').alias('window'))
    cols.remove('start')
    cols.remove('end')
    cols.remove('hour')
    cols.remove('weekday')
    cols.remove('day')
    stress_imputed_data = stress_imputed_data.select(*cols)
    ds = DataStream(data=stress_imputed_data,metadata=get_metadata(stress_imputed_data=stress_imputed_data, output_stream_name=output_stream_name, input_stream_name=stress_data.metadata.get_name()))
    return ds


def impute_stress_likelihood(stress_data,output_stream_name='org.md2k.autosense.ecg.stress.probability.imputed'):
    """


    Args:
        stress_data (DataStream):
        output_stream_name (str):

    Returns:

    """

    def best_fit_slope(ys):
        return np.mean(np.diff(ys))

    def get_trained_model(X_train,y_train):
        paramGrid = {'rf__n_neighbors':[3,4,5,6,7,8,9],
                     }
        clf = Pipeline([('rf',KNeighborsRegressor())])
        gkf = KFold(n_splits=5)
        grid_search = GridSearchCV(clf, paramGrid, n_jobs=-1,cv=gkf.split(X_train),
                                   scoring='r2',verbose=5)
        grid_search.fit(X_train,y_train)
        clf = grid_search.best_estimator_
        clf.fit(X_train,y_train)
        return clf

    weekday_dict = {'Wednesday':5,
                    'Saturday':1,
                    'Thursday':6,
                    'Tuesday':4,
                    'Friday':0,
                    'Sunday':2,
                    'Monday':3}

    schema = StructType([
        StructField("timestamp", DoubleType()),
        StructField("start", DoubleType()),
        StructField("end", DoubleType()),
        StructField("localtime", DoubleType()),
        StructField("version", IntegerType()),
        StructField("user", StringType()),
        StructField("day", StringType()),
        StructField("weekday", StringType()),
        StructField("hour", IntegerType()),
        StructField("stress_probability", DoubleType()),
        StructField("imputed", IntegerType()),
    ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('org.md2k.autosense.ecg.stress.probability.forward.filled', 'impute_stress_likelihood', output_stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def fillup_imputation(data):
        """


        Args:
            data:

        Returns:

        """
        warnings.simplefilter(action="ignore")

        data = data.sort_values('start').reset_index(drop=True)
        X = []
        y = []
        for i,row in data.iterrows():
            if i<lookback:
                continue
            if row['imputed'] in [1]:
                continue
            if 1 not in list(data['imputed'].loc[i-lookback:i].values):
                X.append(np.array(list(data['stress_probability'].iloc[i-lookback:i].values)+
                                  [best_fit_slope(np.array(list(data['stress_probability'].iloc[i-lookback:i].values))),row['hour'],weekday_dict[row['weekday']]]))
                y.append(row['stress_probability'])
        X = np.array(X)
        X_s = X[:,2:4]
        X_hour = X[:,4:5].reshape(-1,1)
        X_weekday = X[:,5:6].reshape(-1,1)
        clf_hour = OneHotEncoder().fit(np.arange(0,25,1).reshape(-1,1))
        clf_week_day = OneHotEncoder().fit(np.arange(0,7,1).reshape(-1,1))
        X_hour = clf_hour.transform(X_hour).todense().reshape(X.shape[0],-1)
        X_weekday = clf_week_day.transform(X_weekday).todense().reshape(X.shape[0],-1)
        X = np.concatenate([X_s,X_hour,X_weekday],axis=1)
        y = np.array(y)
        # print(X.shape)
        clf = get_trained_model(X,y)
        start = data[data.imputed==1].shape[0]+1
        while data[data.imputed==1].shape[0]<start and data[data.imputed==1].shape[0]>0:
            start = data[data.imputed==1].shape[0]
            X = []
            y = []
            index = []
            for i,row in data.iterrows():
                if i<lookback:
                    continue
                if row['imputed'] in [0,2]:
                    continue
                if 1 not in list(data['imputed'].iloc[i-lookback:i].values) and row['imputed']==1:
                    X.append(np.array(list(data['stress_probability'].iloc[i-lookback:i].values)+
                                      [best_fit_slope(np.array(list(data['stress_probability'].iloc[i-lookback:i].values))),row['hour'],weekday_dict[row['weekday']]]))
                    index.append(i)
            if len(X)==0:
                break
            X = np.array(X)
            X_s = X[:,2:4]
            X_hour = X[:,4:5].reshape(-1,1)
            X_weekday = X[:,5:6].reshape(-1,1)
            X_hour = clf_hour.transform(X_hour).todense().reshape(X.shape[0],-1)
            X_weekday = clf_week_day.transform(X_weekday).todense().reshape(X.shape[0],-1)
            X = np.concatenate([X_s,X_hour,X_weekday],axis=1)
            data['stress_probability'].loc[index] = clf.predict(X).reshape(-1)
            data['imputed'].loc[index] = 2
        data['imputed'][data.imputed==2]=1
        return data

    lookback = 3
    stress_data = stress_data.withColumn('day',F.date_format('localtime',"yyyyMMdd"))
    stress_data = stress_data.withColumn('start',F.col('window').start)
    stress_data = stress_data.withColumn('end',F.col('window').end).drop(*['window'])
    stress_data = stress_data.withColumn('start',F.col('start').cast('double'))
    stress_data = stress_data.withColumn('end',F.col('end').cast('double'))
    stress_data = stress_data.withColumn('hour',F.hour('localtime'))
    stress_data = stress_data.withColumn('weekday',F.date_format('localtime','EEEE'))
    stress_data = stress_data.withColumn('localtime',F.col('localtime').cast('double'))
    stress_data = stress_data.withColumn('timestamp',F.col('timestamp').cast('double'))
    stress_imputed_data = stress_data.groupBy(['user','version']).apply(fillup_imputation)
    stress_imputed_data = stress_imputed_data.withColumn('start',F.col('start').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('end',F.col('end').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('localtime',F.col('localtime').cast('timestamp'))
    stress_imputed_data = stress_imputed_data.withColumn('timestamp',F.col('timestamp').cast('timestamp'))
    cols = list(stress_imputed_data.columns)
    cols.append(F.struct('start', 'end').alias('window'))
    cols.remove('start')
    cols.remove('end')
    cols.remove('hour')
    cols.remove('weekday')
    cols.remove('day')
    stress_imputed_data = stress_imputed_data.select(*cols)
    ds = DataStream(data=stress_imputed_data,metadata=get_metadata(stress_imputed_data=stress_imputed_data,output_stream_name=output_stream_name, input_stream_name=stress_data.metadata.get_name()))
    return ds
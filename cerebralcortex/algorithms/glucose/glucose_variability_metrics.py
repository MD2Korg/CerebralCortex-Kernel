# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
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

import pandas as pd
import datetime as datetime
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, ArrayType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata


def glucose_var(ds):
    '''
    Compute CGM Glucose Variability Metrics:

    This algorithm computes 20+ clinically validated glucose variability metrics from continuous glucose monitor data.
    Please see dbdp.org for more documentation on these functions.

        * glucose_var() - input is datastream of CGM data; returns datastream of glucose variability metrics + metadata
        * import_data() - formats datastream as pandas dataframe for use with other functions
        * interdaycv() - computes the interday coefficient of variation on pandas dataframe glucose column
        * interdaysd() - computes the interday standard deviation of pandas dataframe glucose column
        * intradaycv() - computes the intradaycv, returns the mean, median, and sd of intraday cv glucose column in pandas dataframe
        * intradaysd() - computes the intradaysd, returns the mean, median, and sd of intraday sd glucose column in pandas dataframe
        * TIR() - computes time in the range of (default=1 sd from the mean) glucose column in pandas dataframe
        * TOR() - computes time outside the range of (default=1 sd from the mean) glucose column in pandas dataframe
        * POR() - computes percent time outside the range of (default=1 sd from the mean) glucose column in pandas dataframe
        * MAGE() - computes the mean amplitude of glucose excursions (default = 1 sd from the mean)
        * MAGN() - computes the mean amplitude of normal glucose (default = 1 sd from the mean)
        * J_index() - computes the J index, a parameter of the mean and standard deviation of glucose
        * LBGI_HBGI() - computes the LBGI, HBGI, rh, and rl of glucose
        * LBGI() - computes LBGI of glucose
        * HBGI() - computes HBGI of glucose
        * ADRR() - computes ADRR of glucose (requires function LBGI_HBGI to calculate rh and rl parameters
        * uniquevalfilter() - supporting function for MODD and CONGA24 calculations
        * MODD() - computes mean of daily differences of glucose
        * CONGA24() - computes CONGA over 24 hour interval
        * GMI() - computes glucose management index
        * eA1c() - computes ADA estimated A1c from glucose
        * summary() - computes interday mean glucose, median glucose, minimum and maximum glucose, and first and third quartile of glucose
        * get_all_metrics() - compiles all above metrics into a dataframe with return_schema
    Input:
        ds (DataStream): datastream object
    Returns:
        DataStream with glucose variability metrics
    '''


    schema = ds.data.schema

    features_list = [StructField('Timestamp (YYYY-MM-DDThh:mm:ss)', TimestampType()),
                     StructField('Glucose Value (mg/dL)', IntegerType())]

    data_names = [a.name for a in features_list]
    columns = []
    for c in data_names:
        if c in columns:
            ds = ds.drop(*[c])
    schema = StructType(ds._data.schema.fields + features_list)
    column_names = [a.name for a in schema.fields]

    # TODO: not sure what is the use of this method.
    # @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    # def import_data(data):
    #     df['Time'] = data['Timestamp (YYYY-MM-DDThh:mm:ss)']
    #     df['Glucose'] = pd.to_numeric(data['Glucose Value (mg/dL)'])
    #     df.drop(df.index[:12], inplace=True)
    #     df['Time'] = pd.to_datetime(df['Time'], format='%Y-%m-%dT%H:%M:%S')
    #     df['Day'] = df['Time'].dt.date
    #     df = df.reset_index()
    #     return df


    def interdayCV(df):
        cvx = (np.std(df['Glucose']) / (np.mean(df['Glucose']))) * 100
        return cvx


    def interdaySD(df):
        interdaysd = np.std(df['Glucose'])
        return interdaysd


    def intradayCV(df):
        intradaycv = []
        for i in pd.unique(df['Day']):
            intradaycv.append(interdayCV(df[df['Day'] == i]))

        intradaycv_mean = np.mean(intradaycv)
        intradaycv_median = np.median(intradaycv)
        intradaycv_sd = np.std(intradaycv)

        return intradaycv_mean, intradaycv_median, intradaycv_sd


    def intradaySD(df):
        intradaysd = []

        for i in pd.unique(df['Day']):
            intradaysd.append(np.std(df[df['Day'] == i]))

        intradaysd_mean = np.mean(intradaysd)
        intradaysd_median = np.median(intradaysd)
        intradaysd_sd = np.std(intradaysd)
        return intradaysd_mean, intradaysd_median, intradaysd_sd


    def TIR(df, sd=1, sr=5):
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        TIR = len(df[(df['Glucose'] <= up) & (df['Glucose'] >= dw)]) * sr
        return TIR


    def TOR(df, sd=1, sr=5):
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        TOR = len(df[(df['Glucose'] >= up) | (df['Glucose'] <= dw)]) * sr
        return TOR


    def POR(df, sd=1, sr=5):
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        TOR = len(df[(df['Glucose'] >= up) | (df['Glucose'] <= dw)]) * sr
        POR = (TOR / (len(df) * sr)) * 100
        return POR


    def MAGE(df, sd=1):
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        MAGE = np.mean(df[(df['Glucose'] >= up) | (df['Glucose'] <= dw)])
        return MAGE


    def MAGN(df, sd=1):
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        MAGN = np.mean(df[(df['Glucose'] <= up) & (df['Glucose'] >= dw)])
        return MAGN


    def J_index(df):
        J = 0.001 * ((np.mean(df['Glucose']) + np.std(df['Glucose'])) ** 2)
        return J


    def LBGI_HBGI(df):
        f = ((np.log(df['Glucose']) ** 1.084) - 5.381)
        rl = []
        for i in f:
            if (i <= 0):
                rl.append(22.77 * (i ** 2))
            else:
                rl.append(0)

        LBGI = np.mean(rl)

        rh = []
        for i in f:
            if (i > 0):
                rh.append(22.77 * (i ** 2))
            else:
                rh.append(0)

        HBGI = np.mean(rh)

        return LBGI, HBGI, rh, rl


    def LBGI(df):
        f = ((np.log(df['Glucose']) ** 1.084) - 5.381)
        rl = []
        for i in f:
            if (i <= 0):
                rl.append(22.77 * (i ** 2))
            else:
                rl.append(0)

        LBGI = np.mean(rl)
        return LBGI


    def HBGI(df):
        f = ((np.log(df['Glucose']) ** 1.084) - 5.381)
        rh = []
        for i in f:
            if (i > 0):
                rh.append(22.77 * (i ** 2))
            else:
                rh.append(0)

        HBGI = np.mean(rh)
        return HBGI


    def ADRR(df):
        ADRRl = []
        for i in pd.unique(df['Day']):
            LBGI, HBGI, rh, rl = LBGI_HBGI(df[df['Day'] == i])
            LR = np.max(rl)
            HR = np.max(rh)
            ADRRl.append(LR + HR)

        ADRRx = np.mean(ADRRl)
        return ADRRx


    def uniquevalfilter(df, value):
        xdf = df[df['Minfrommid'] == value]
        n = len(xdf)
        diff = abs(xdf['Glucose'].diff())
        MODD_n = np.nanmean(diff)
        return MODD_n


    def MODD(df):
        df['Timefrommidnight'] = df['Time'].dt.time
        lists = []
        for i in range(0, len(df['Timefrommidnight'])):
            lists.append(int(df['Timefrommidnight'][i].strftime('%H:%M:%S')[0:2]) * 60 + int(
                df['Timefrommidnight'][i].strftime('%H:%M:%S')[3:5]) + round(
                int(df['Timefrommidnight'][i].strftime('%H:%M:%S')[6:9]) / 60))
        df['Minfrommid'] = lists
        df = df.drop(columns=['Timefrommidnight'])

        # Calculation of MODD and CONGA:
        MODD_n = []
        uniquetimes = df['Minfrommid'].unique()

        for i in uniquetimes:
            MODD_n.append(uniquevalfilter(df, i))

        # Remove zeros from dataframe for calculation (in case there are random unique values that result in a mean of 0)
        MODD_n[MODD_n == 0] = np.nan

        MODD = np.nanmean(MODD_n)
        return MODD


    def CONGA24(df):
        df['Timefrommidnight'] = df['Time'].dt.time
        lists = []
        for i in range(0, len(df['Timefrommidnight'])):
            lists.append(int(df['Timefrommidnight'][i].strftime('%H:%M:%S')[0:2]) * 60 + int(
                df['Timefrommidnight'][i].strftime('%H:%M:%S')[3:5]) + round(
                int(df['Timefrommidnight'][i].strftime('%H:%M:%S')[6:9]) / 60))
        df['Minfrommid'] = lists
        df = df.drop(columns=['Timefrommidnight'])

        # Calculation of MODD and CONGA:
        MODD_n = []
        uniquetimes = df['Minfrommid'].unique()

        for i in uniquetimes:
            MODD_n.append(uniquevalfilter(df, i))

        # Remove zeros from dataframe for calculation (in case there are random unique values that result in a mean of 0)
        MODD_n[MODD_n == 0] = np.nan

        CONGA24 = np.nanstd(MODD_n)
        return CONGA24


    def GMI(df):
        GMI = 3.31 + (0.02392 * np.mean(df['Glucose']))
        return GMI


    def eA1c(df):
        eA1c = (46.7 + np.mean(df['Glucose'])) / 28.7
        return eA1c


    def summary(df):
        meanG = np.nanmean(df['Glucose'])
        medianG = np.nanmedian(df['Glucose'])
        minG = np.nanmin(df['Glucose'])
        maxG = np.nanmax(df['Glucose'])
        Q1G = np.nanpercentile(df['Glucose'], 25)
        Q3G = np.nanpercentile(df['Glucose'], 75)

        return meanG, medianG, minG, maxG, Q1G, Q3G


    return_schema = StructType([
        StructField("meanG", IntegerType()),
        StructField("medianG", IntegerType()),
        StructField("minG", IntegerType()),
        StructField("maxG", IntegerType()),
        StructField("Q1G", IntegerType()),
        StructField("Q3G", IntegerType()),
        StructField("GMI", IntegerType()),
        StructField("eA1c", IntegerType()),
        StructField("CONGA24", IntegerType()),
        StructField("MODD", IntegerType()),
        StructField("ADRR", IntegerType()),
        StructField("HBGI", IntegerType()),
        StructField("LBGI", IntegerType()),
        StructField("J-index", IntegerType()),
        StructField("MAGE", IntegerType()),
        StructField("MAGN", IntegerType()),
        StructField("POR", IntegerType()),
        StructField("TOR", IntegerType()),
        StructField("TIR", IntegerType()),
        StructField("interdaySD", IntegerType()),
        StructField("interdayCV", IntegerType()),
        StructField("intradaySD", IntegerType()),
        StructField("intradayCV", IntegerType())
    ])


    @pandas_udf(return_schema, PandasUDFType.GROUPED_MAP)
    def get_all_metrics(data):
        '''
        Compute CGM Metrics
        Input:
            Pandas data frame of raw continuous gluose monitor data
        Returns:
            Pandas data frame of CGM summary and variability metrics
        '''

        df = pd.DataFrame()
        output = pd.DataFrame()
        df['Time'] = data['Timestamp (YYYY-MM-DDThh:mm:ss)']
        df['Glucose'] = pd.to_numeric(data['Glucose Value (mg/dL)'])
        df.drop(df.index[:12], inplace=True)
        df['Time'] = pd.to_datetime(df['Time'], format='%Y-%m-%dT%H:%M:%S')
        df['Day'] = df['Time'].dt.date
        df = df.reset_index()

        meanG, medianG, minG, maxG, Q1G, Q3G = summary(df)
        intradaysd, intradaySDmedian, intradaySDSD = intradaySD(df)
        intradaycv, intradayCVmedian, intradayCVSD = intradayCV(df)

        output['meanG'] = meanG
        output['medianG'] = medianG
        output['minG'] = minG
        output['maxG'] = maxG
        output['Q1G'] = Q1G
        output['Q3G'] = Q3G
        output['GMI'] = GMI(df)
        output['eA1c'] = eA1c(df)
        output['CONGA24'] = CONGA24(df)
        output['MODD'] = MODD(df)
        output['ADRR'] = ADRR(df)
        output['HBGI'] = HBGI(df)
        output['LBGI'] = LBGI(df)
        output['J-index'] = J_index(df)
        output['MAGE'] = MAGE(df)
        output['MAGN'] = MAGN(df)
        output['POR'] = POR(df)
        output['TOR'] = TOR(df)
        output['TIR'] = TIR(df)
        output['interdaySD'] = interdaySD(df)
        output['interdayCV'] = interdayCV(df)
        output['intradaySD'] = intradaysd
        output['intradayCV'] = intradaycv
        return output


    data = ds._data.apply(get_all_metrics)
    return DataStream(data=data, metadata=Metadata())
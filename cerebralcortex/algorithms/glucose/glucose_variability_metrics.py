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

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.group import GroupedData
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType

from cerebralcortex.algorithms.utils.util import update_metadata
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata

def glucose_var(ds):
    """

    Compute CGM Glucose Variability Metrics:

    This algorithm computes 23 clinically validated glucose variability metrics from continuous glucose monitor data.

    Input:
        ds (DataStream): Windowed/grouped DataStream of CGM data

    Returns:
        DataStream with glucose variability metrics
        Glucose Variability Metrics include: 
        Interday Mean Glucose
        Interday Median Glucose
        Interday Maximum Glucose
        Interday Minimum Glucose
        Interday Standard Deviation of Glucose
        Interday Coefficient of Variation of Glucose
        Intraday Standard Deviation of Glucose (mean, median, standard deviation)
        Intraday Coefficient of Variation of Glucose (mean, median, standard deviation)
        TIR (Time in Range of default 1 SD)
        TOR (Time outside Range of default 1 SD)
        POR (Percent outside Range of default 1 SD)
        MAGE (Mean Amplitude of Glucose Excursions, default 1 SD)
        MAGN (Mean Amplitude of Normal Glucose, default 1 SD)
        J-index
        LBGI (Low Blood Glucose Index)
        HBGI (High Blood Glucose Index)
        MODD (Mean of Daily Differences)
        CONGA24 (Continuous overall net glycemic action over 24 hours)
        ADRR (Average Daily Risk Range)
        GMI (Glucose Management Indicator)
        eA1c (estimated A1c according to American Diabetes Association)
        Q1G (intraday first quartile glucose)
        Q3G (intraday third quartile glucose)
        ** for more information on these glucose metrics see dbdp.org**
        
    """

    def interdayCV(df):
        """
        computes the interday coefficient of variation on pandas dataframe glucose column

        Args:
            df (pandas.DataFrame):

        Returns:
            cvx (IntegerType): interday coefficient of variation of glucose

        """
        cvx = (np.std(df['Glucose']) / (np.mean(df['Glucose']))) * 100
        return cvx

    def interdaySD(df):
        """
        computes the interday standard deviation of pandas dataframe glucose column
        Args:
             df (pandas.DataFrame):

        Returns:
            interdaysd (IntegerType): interday standard deviation of glucose

        """
        interdaysd = np.std(df['Glucose'])
        return interdaysd

    def intradayCV(df):
        """
        computes the intradaycv, returns the mean, median, and sd of intraday cv glucose column in pandas dataframe
        Args:
             df (pandas.DataFrame):

        Returns:
            intradaycv_mean (IntegerType): Mean, Median, and SD of intraday coefficient of variation of glucose
            intradaycv_median (IntegerType): Median of intraday coefficient of variation of glucose
            intradaycv_sd (IntegerType): SD of intraday coefficient of variation of glucose

        """
        intradaycv = []
        for i in pd.unique(df['Day']):
            intradaycv.append(interdayCV(df[df['Day'] == i]))

        intradaycv_mean = np.mean(intradaycv)
        intradaycv_median = np.median(intradaycv)
        intradaycv_sd = np.std(intradaycv)

        return intradaycv_mean, intradaycv_median, intradaycv_sd

    def intradaySD(df):
        """
        computes the intradaysd, returns the mean, median, and sd of intraday sd glucose column in pandas dataframe
        Args:
             df (pandas.DataFrame):

        Returns:
            intradaysd_mean (IntegerType): Mean, Median, and SD of intraday standard deviation of glucose
            intradaysd_median (IntegerType): Median of intraday standard deviation of glucose
            intradaysd_sd (IntegerType): SD of intraday standard deviation of glucose

        """
        intradaysd = []

        for i in pd.unique(df['Day']):
            intradaysd.append(np.std(df[df['Day'] == i]))

        intradaysd_mean = np.mean(intradaysd)
        intradaysd_median = np.median(intradaysd)
        intradaysd_sd = np.std(intradaysd)
        return intradaysd_mean, intradaysd_median, intradaysd_sd

    def TIR(df, sd=1, sr=5):
        """
        computes time in the range of (default=1 sd from the mean) glucose column in pandas dataframe
        Args:
             df (pandas.DataFrame):
             sd (IntegerType): standard deviation from mean for range calculation (default = 1 SD)
             sr (IntegerType): Number of minutes between measurements on CGM (default: 5 minutes, standard sampling rate of devices)

        Returns:
            TIR (IntegerType): Time in Range set by sd
            

        """
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        TIR = len(df[(df['Glucose'] <= up) & (df['Glucose'] >= dw)]) * sr
        return TIR

    def TOR(df, sd=1, sr=5):
        """
        computes time outside the range of (default=1 sd from the mean) glucose column in pandas dataframe
        Args:
             df (pandas.DataFrame):
             sd (IntegerType): standard deviation from mean for range calculation (default = 1 SD)
             sr (IntegerType): Number of minutes between measurements on CGM (default: 5 minutes, standard sampling rate of devices)

        Returns:
            TOR (IntegerType): Time outside of range set by sd

        """
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        TOR = len(df[(df['Glucose'] >= up) | (df['Glucose'] <= dw)]) * sr
        return TOR

    def POR(df, sd=1, sr=5):
        """
        computes percent time outside the range of (default=1 sd from the mean) glucose column in pandas dataframe
        Args:
             df (pandas.DataFrame):
             sd (IntegerType): standard deviation from mean for range calculation (default = 1 SD)
             sr (IntegerType): Number of minutes between measurements on CGM (default: 5 minutes, standard sampling rate of devices)

        Returns:
            POR (IntegerType): percent of time spent outside range set by sd

        """
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        TOR = len(df[(df['Glucose'] >= up) | (df['Glucose'] <= dw)]) * sr
        POR = (TOR / (len(df) * sr)) * 100
        return POR

    def MAGE(df, sd=1):
        """
        computes the mean amplitude of glucose excursions (default = 1 sd from the mean)
        Args:
             df (pandas.DataFrame):
             sd (IntegerType): standard deviation from mean to set as a glucose excursion (default = 1 SD)

        Returns:
           MAGE (IntegerType): Mean Amplitude of glucose excursions

        """
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        MAGE = np.mean((df['Glucose'] >= up) | (df['Glucose'] <= dw))
        return MAGE

    def MAGN(df, sd=1):
        """
        computes the mean amplitude of normal glucose (default = 1 sd from the mean)
        Args:
             df (pandas.DataFrame):
             sd (IntegerType): standard deviation from mean to set as a glucose excursion (default = 1 SD)

        Returns:
           MAGN (IntegerType):  Mean Amplitude of Normal Glucose

        """
        up = np.mean(df['Glucose']) + sd * np.std(df['Glucose'])
        dw = np.mean(df['Glucose']) - sd * np.std(df['Glucose'])
        MAGN = np.mean((df['Glucose'] <= up) & (df['Glucose'] >= dw))
        return MAGN

    def J_index(df):
        """
        computes the J index, a parameter of the mean and standard deviation of glucose
        Args:
             df (pandas.DataFrame):

        Returns:
            J (IntegerType): The J-index, a metric of GV that is a parameter of the mean and standard deviation of glucose

        """
        J = 0.001 * ((np.mean(df['Glucose']) + np.std(df['Glucose'])) ** 2)
        return J

    def LBGI_HBGI(df):
        """
        This is an intermediary function. This is needed for below functions. Please do not use this function on its own.
        computes the LBGI, HBGI, rh, and rl of glucose
        Args:
             df (pandas.DataFrame):

        Returns:
            LBGI (IntegerType): Do not use
            HBGI (IntegerType): Do not use
            rh (IntegerType): rh of glucose, supporting calculation for LBGI, HBGI, ADRR functions
            rl (IntegerType): rl of glucose, supporting calculation for LBGI, HBGI, ADRR functions

        """
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
        """
        computes LBGI of glucose
        Args:
             df (pandas.DataFrame):

        Returns:
            LBGI (IntegerType): Low Blood Glucose Index (metric of hypoglycemic risk)

        """
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
        """
        computes HBGI of glucose
        Args:
             df (pandas.DataFrame):

        Returns:
            HBGI (IntegerType): High Blood Glucose Index (metric of hyperglycemia risk)

        """
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
        """
        computes ADRR of glucose (requires function LBGI_HBGI to calculate rh and rl parameters)
        Args:
             df (pandas.DataFrame):

        Returns:
            ADRRx (IntegerType): Average Daily Risk Range (an assesment of total daily glucose variations within a specific risk space, given by rh and rl)

        """
        ADRRl = []
        for i in pd.unique(df['Day']):
            LBGI, HBGI, rh, rl = LBGI_HBGI(df[df['Day'] == i])
            LR = np.max(rl)
            HR = np.max(rh)
            ADRRl.append(LR + HR)

        ADRRx = np.mean(ADRRl)
        return ADRRx

    def uniquevalfilter(df, value):
        """
        supporting function for MODD and CONGA24 calculations
        Args:
             df (pandas.DataFrame):
            value (IntegerType): a specific timepoint from the data frame given by MODD or CONGA24 function 

        Returns:
            MODD_n (IntegerType): supporting calculation for MODD and CONGA24

        """
        xdf = df[df['Minfrommid'] == value]
        n = len(xdf)
        diff = abs(xdf['Glucose'].diff())
        MODD_n = np.nanmean(diff)
        return MODD_n

    def MODD(df):
        """
        computes mean of daily differences of glucose
        Args:
             df (pandas.DataFrame):

        Returns:
           MODD (IntegerType): Mean of Daily Differences, a measure of cyrccadian rhythmicity of glucose variability 

        """
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
        """
        computes CONGA over 24 hour interval
        Args:
             df (pandas.DataFrame):

        Returns:
           CONGA24 (IntegerType): continuous overall net glycemic action over 24 hours

        """
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
        """
        computes glucose management index
        Args:
             df (pandas.DataFrame):

        Returns:
            GMI (IntegerType): glucose management index

        """
        GMI = 3.31 + (0.02392 * np.mean(df['Glucose']))
        return GMI

    def eA1c(df):
        """
        computes ADA estimated A1c from glucose
        Args:
             df (pandas.DataFrame):

        Returns:
           eA1c (IntegerType): the estimated A1c according to American Diabetes Association algorithm

        """
        eA1c = (46.7 + np.mean(df['Glucose'])) / 28.7
        return eA1c

    def summary(df):
        """
        computes interday mean glucose, median glucose, minimum and maximum glucose, and first and third quartile of glucose
        Args:
             df (pandas.DataFrame):

        Returns:
            meanG (FloatType): mean glucose
            medianG (FloatType): median glucose
            minG (FloatType): minimum glucose
            maxG (FloatType): maximum glucose
            Q1G (FloatType): first quartile glucose
            Q3G (FloatType): third quartile glucose

        """
        meanG = np.nanmean(df['Glucose'])
        medianG = np.nanmedian(df['Glucose'])
        minG = np.nanmin(df['Glucose'])
        maxG = np.nanmax(df['Glucose'])
        Q1G = np.nanpercentile(df['Glucose'], 25)
        Q3G = np.nanpercentile(df['Glucose'], 75)
        return meanG, medianG, minG, maxG, Q1G, Q3G

    return_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("user", StringType()),
        StructField("version", IntegerType()),
        StructField("meanG", FloatType()),
        StructField("medianG", FloatType()),
        StructField("minG", FloatType()),
        StructField("maxG", FloatType()),
        StructField("Q1G", FloatType()),
        StructField("Q3G", FloatType()),
        StructField("GMI", IntegerType()),
        StructField("eA1c", IntegerType()),
        StructField("CONGA24", IntegerType()),
        StructField("MODD", IntegerType()),
        StructField("ADRR", IntegerType()),
        StructField("HBGI", IntegerType()),
        StructField("LBGI", IntegerType()),
        StructField("J_index", IntegerType()),
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
        output = []
        df['Time'] = data['timestamp']
        df['Glucose'] = pd.to_numeric(data['glucose_value'])
        df.drop(df.index[:12], inplace=True)
        df['Time'] = pd.to_datetime(df['Time'], format='%Y-%m-%dT%H:%M:%S')
        df['Day'] = df['Time'].dt.date
        df = df.reset_index()

        meanG, medianG, minG, maxG, Q1G, Q3G = summary(df)
        intradaysd, intradaySDmedian, intradaySDSD = intradaySD(df)
        intradaycv, intradayCVmedian, intradayCVSD = intradayCV(df)

        output.append(data.timestamp.iloc[0])
        output.append(data.localtime.iloc[0])
        output.append(data.user.iloc[0])
        output.append(1)

        output.append(meanG)
        output.append(medianG)
        output.append(minG)
        output.append(maxG)
        output.append(Q1G)
        output.append(Q3G)
        output.append(GMI(df))
        output.append(eA1c(df))
        output.append(CONGA24(df))
        output.append(MODD(df))
        output.append(ADRR(df))
        output.append(HBGI(df))
        output.append(LBGI(df))
        output.append(J_index(df))
        output.append(MAGE(df))
        output.append(MAGN(df))
        output.append(POR(df))
        output.append(TOR(df))
        output.append(TIR(df))
        output.append(interdaySD(df))
        output.append(interdayCV(df))
        output.append(intradaysd)
        output.append(intradaycv)
        column_names = [a.name for a in return_schema]
        pdf = pd.DataFrame([output], columns=column_names)
        return pdf

    # check if datastream object contains grouped type of DataFrame
    # if not isinstance(ds._data, GroupedData):
    #     raise Exception(
    #         "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")



    data = ds._data.groupBy(["user", "version"]).apply(get_all_metrics)

    results = DataStream(data=data, metadata=Metadata())
    metadta = update_metadata(stream_metadata=results.metadata,
                    stream_name="cgm_glucose_variability_metrics",
                    stream_desc="This algorithm computes 23 clinically validated glucose variability metrics from continuous glucose monitor data. Datastream input is CGM data containing timestamp and glucose. Datastream output is 23 glucose variability metrics.",
                    module_name="cerebralcortex.algorithms.glucose.glucose_variability_metrics.glucose_var",
                    module_version="1.0.0",
                    authors=[{"Digital Biomarker Discovery Pipeline (DBDP)":"brinnae.bent@duke.edu"}])
    results.metadata = metadta
    return results

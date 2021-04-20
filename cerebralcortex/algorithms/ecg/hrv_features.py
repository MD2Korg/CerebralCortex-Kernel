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

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, ArrayType, TimestampType, IntegerType
from scipy import signal
from scipy.stats import iqr

from cerebralcortex.algorithms.utils.mprov_helper import CC_MProvAgg
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, \
    ModuleMetadata


def get_hrv_features(rr_data,
                     acceptable_percentage=50,
                     window_length=60):
    """

    Args:
        rr_data (DataStream):
        acceptable_percentage (int):
        window_length (int):

    Returns:

    """
    stream_name = 'org.md2k.autosense.ecg.features'

    def get_metadata():
        stream_metadata = Metadata()
        stream_metadata.set_name(stream_name).set_description("HRV Features from ECG RR interval") \
            .add_input_stream(rr_data.metadata.get_name()) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("var")
                .set_type("double")
                .set_attribute("description","variance")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("iqr")
                .set_type("double")
                .set_attribute("description","Inter Quartile Range")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("mean")
                .set_type("double")
                .set_attribute("description","Mean RR Interval")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("median")
                .set_type("double")
                .set_attribute("description","Median RR Interval")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("80th")
                .set_type("double")
                .set_attribute("description","80th percentile RR Interval")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("20th")
                .set_type("double")
                .set_attribute("description","20th percentile RR Interval")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("heartrate")
                .set_type("double")
                .set_attribute("description","Heart Rate in BPM")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("vlf")
                .set_type("double")
                .set_attribute("description","Very Low Frequency Energy")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("lf")
                .set_type("double")
                .set_attribute("description","Low Frequency Energy")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("hf")
                .set_type("double")
                .set_attribute("description","High Frequency Energy")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("lfhf")
                .set_type("double")
                .set_attribute("description","Low frequency to High Frequency energy ratio")) \
            .add_dataDescriptor(
            DataDescriptor()
                .set_name("window")
                .set_type("struct")
                .set_attribute("description","window start and end time in UTC")
                .set_attribute('start','start of window')
                .set_attribute('end','end of window')) \
            .add_module(
            ModuleMetadata().set_name("HRV Features from ECG RR Interval")
                .set_attribute("url", "http://md2k.org/")
                .set_attribute('algorithm','ecg feature computation')
                .set_attribute('unit','ms')
                .set_author("Md Azim Ullah", "mullah@memphis.edu"))
        return stream_metadata

    def get_rr_features(a):
        return np.array([np.var(a),iqr(a),np.mean(a),np.median(a),np.percentile(a,80),np.percentile(a,20),60000/np.median(a)])

    def frequencyDomain(RRints,
                        tmStamps,
                        band_type = None,
                        lf_bw = 0.11,
                        hf_bw = 0.1,
                        vlf= (0.003, 0.04),
                        lf = (0.04, 0.15),
                        hf = (0.15, 0.4)):
        """

        Args:
            RRints:
            tmStamps:
            band_type:
            lf_bw:
            hf_bw:
            vlf:
            lf:
            hf:

        Returns:

        """
        NNs = RRints
        tss = tmStamps
        frequency_range = np.linspace(0.001, 1, 10000)
        NNs = np.array(NNs)
        NNs = NNs - np.mean(NNs)
        result = signal.lombscargle(tss, NNs, frequency_range)

        #Pwelch w/ zero pad
        fxx = frequency_range
        pxx = result

        if band_type == 'adapted':

            vlf_peak = fxx[np.where(pxx == np.max(pxx[np.logical_and(fxx >= vlf[0], fxx < vlf[1])]))[0][0]]
            lf_peak = fxx[np.where(pxx == np.max(pxx[np.logical_and(fxx >= lf[0], fxx < lf[1])]))[0][0]]
            hf_peak = fxx[np.where(pxx == np.max(pxx[np.logical_and(fxx >= hf[0], fxx < hf[1])]))[0][0]]

            peak_freqs =  (vlf_peak, lf_peak, hf_peak)

            hf = (peak_freqs[2] - hf_bw/2, peak_freqs[2] + hf_bw/2)
            lf = (peak_freqs[1] - lf_bw/2, peak_freqs[1] + lf_bw/2)
            vlf = (0.003, lf[0])

            if lf[0] < 0:
                print('***Warning***: Adapted LF band lower bound spills into negative frequency range')
                print('Lower thresold of LF band has been set to zero')
                print('Adjust LF and HF bandwidths accordingly')
                lf = (0, lf[1])
                vlf = (0, 0)
            elif hf[0] < 0:
                print('***Warning***: Adapted HF band lower bound spills into negative frequency range')
                print('Lower thresold of HF band has been set to zero')
                print('Adjust LF and HF bandwidths accordingly')
                hf = (0, hf[1])
                lf = (0, 0)
                vlf = (0, 0)

        df = fxx[1] - fxx[0]
        vlf_power = np.trapz(pxx[np.logical_and(fxx >= vlf[0], fxx < vlf[1])], dx = df)
        lf_power = np.trapz(pxx[np.logical_and(fxx >= lf[0], fxx < lf[1])], dx = df)
        hf_power = np.trapz(pxx[np.logical_and(fxx >= hf[0], fxx < hf[1])], dx = df)
        totalPower = vlf_power + lf_power + hf_power

        #Normalize and take log
        vlf_NU_log = np.log((vlf_power / (totalPower - vlf_power)) + 1)
        lf_NU_log = np.log((lf_power / (totalPower - vlf_power)) + 1)
        hf_NU_log = np.log((hf_power / (totalPower - vlf_power)) + 1)
        lfhfRation_log = np.log((lf_power / hf_power) + 1)

        freqDomainFeats = {'VLF_Power': vlf_NU_log, 'LF_Power': lf_NU_log,
                           'HF_Power': hf_NU_log, 'LF/HF': lfhfRation_log}

        return freqDomainFeats

    schema = StructType([StructField("timestamp", TimestampType()),
                         StructField("start", TimestampType()),
                         StructField("end", TimestampType()),
                         StructField("localtime", TimestampType()),
                         StructField("version", IntegerType()),
                         StructField("user", StringType()),
                         StructField("features", ArrayType(DoubleType()))
                         ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    @CC_MProvAgg('org.md2k.autosense.ecg.rr', 'get_hrv_features', stream_name, ['user', 'timestamp'], ['user', 'timestamp'])
    def ecg_r_peak(key,data):
        """

        Args:
            key:
            data:

        Returns:

        """
        if data.shape[0]>=acceptable_percentage*window_length/100:
            data = data.sort_values('time')
            data['time'] = 1000*data['time']
            a = data['rr'].values
            features = [np.double(np.array(list(get_rr_features(a))+list(frequencyDomain(np.array(a)/1000,np.cumsum(a)/1000).values())))]
            data = data[:1]
            data['features'] = features
            data['start'] = [key[2]['start']]
            data['end'] = [key[2]['end']]
            data = data[['timestamp','localtime','version','user','start','end','features']]
            return data
        else:
            return pd.DataFrame([],columns=['timestamp','localtime','version','user','features','start','end'])

    rr_data = rr_data.withColumn('time',F.col('timestamp').cast('double'))
    ecg_features = rr_data.compute(ecg_r_peak,windowDuration=window_length,startTime='0 seconds')
    df = ecg_features.select('timestamp', F.struct('start', 'end').alias('window'), 'localtime','features','user','version')
    df = df.withColumn('var', F.col('features').getItem(0))
    df = df.withColumn('iqr', F.col('features').getItem(1))
    df = df.withColumn('vlf', F.col('features').getItem(7))
    df = df.withColumn('lf', F.col('features').getItem(8))
    df = df.withColumn('hf', F.col('features').getItem(9))
    df = df.withColumn('lfhf', F.col('features').getItem(10))
    df = df.withColumn('mean', F.col('features').getItem(2))
    df = df.withColumn('median', F.col('features').getItem(3))
    df = df.withColumn('80th', F.col('features').getItem(4))
    df = df.withColumn('20th', F.col('features').getItem(5))
    ecg_features_final = df.withColumn('heartrate', F.col('features').getItem(6))
    ecg_features_final = ecg_features_final.drop('features')

    feature_names = ['var','iqr','mean','median','80th','20th','heartrate','vlf','lf','hf','lfhf']
    stress_features = ecg_features_final.withColumn('features',F.array([F.col(i) for i in feature_names]))
    stress_features.metadata = get_metadata()

    return stress_features
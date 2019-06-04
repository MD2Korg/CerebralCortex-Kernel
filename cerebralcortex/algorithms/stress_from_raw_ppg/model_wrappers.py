import numpy as np
import pickle
import sys
import os
import pandas as pd


class AvailabilityModel:
    """
    AvailabilityModel wraps an availability prediction model and associated preprocessing code.
    When the class is initialized, it will load the model to be wrapped from the data directory.
    The model can be loaded either 1) from a pickle file or 2) from a model class -- see examples
    in __init__().

    The goal is to encapsulate the preprocessing and prediction code behind a fixed interface.

    See EXAMPLE CODE section at the bottom of the file for usage.
    """

    def __init__(self, data_folder_path):
        """
        Load models here so they don't need to be loaded from scratch each time predict()
        is called.

        Args:
            data_folder_path (string): path to folder containing stored models, processing files, etc.

        """
        # add data directory to path for importing model classes or preprocessing functions
        sys.path.append(data_folder_path)

        # TODO: import model (see examples below)

        # EXAMPLE: load a pickled model from the data directory like this:
        model_file = 'logistic_regression'
        self.model = pickle.load(open(os.path.join(data_folder_path, model_file), 'rb'))




    def predict(self,activity_labels, home):
        """
        Take a window's worth of data, perform any necessary processing on it, and return a
        prediction.  Note that raw location data are not being transmitted from the device,
        so location readings are booleans (True means the user is home)

        Args:
            sequence_numbers (int array):   array of sequence numbers from the MotionSense HRV
            accel_data (float array):       array of accelerometer readings from the MotionSense HRV
            activity_labels (string array): array of Google Activity API labels
            home (boolean array):           array of booleans identifying whether a user is home

        Returns:
            yhat:                           availability predictions

        """
        # TODO: replace code below with implementation

        # preprocess incoming data

        X = self.preprocessing_activity( activity_labels, home)

        # make and return predictions
        yhat = self.model.predict([X])
        return yhat

    def  preprocessing_activity(self, activity_labels, home):
        """Prepare data for prediction"""

        # TODO: replace the following with any necessary preprocessing calls:

        # import preprocessing functions from modules in the data directory
        from cerebralcortex.algorithms.stress_from_raw_ppg.preprocessing_availability import preprocess_activity_data

        # process some data
        processed_data = preprocess_activity_data(activity_labels,home)

        # return processed data
        return processed_data




class StressModel:
    """
    StressModel wraps a stress prediction model and associated preprocessing code.
    When the class is initialized, it will load the model to be wrapped from the data directory.
    The model can be loaded either 1) from a pickle file or 2) from a model class -- see examples
    in __init__().

    The goal is to encapsulate the preprocessing and prediction code behind a fixed interface.

    See EXAMPLE CODE section at the bottom of the file for usage.
    """

    def __init__(self, data_folder_path):
        """
        Load models here so they don't need to be loaded from scratch each time predict()
        is called.

        Args:
            data_folder_path (string): path to folder containing stored models, processing files, etc.

        """
        # add data directory to path for importing model classes or preprocessing functions
        sys.path.append(data_folder_path)

        # TODO: import model (see examples below)
        self.running_mean = []
        self.running_std = []
        self.look_back = 6 * 60 * 60 * 1000 # 6 hours time to look back (in ms)
        # EXAMPLE: load a pickled model from the data directory like this:
        model_file = 'stress_clf.p'
        self.names = ['var','vlf','hf','lf','lf-hf','mean','median','iqr','hr_med','80_perc','20_perc']
        self.model = pickle.load(open(os.path.join(data_folder_path, model_file), 'rb'))

    def predict(self,
                raw_data = None,
                sequence_numbers=None,
                accel_data=None,
                gyro_data=None,
                ppg_data=None,
                acceptable_fraction=.6,
                Fs=25,
                window_size=65):
        """
        Take a window's worth of data, perform any necessary processing on it, and return a
        prediction.  Note that raw location data are not being transmitted from the device,
        so location readings are booleans (True means the user is home)

        Args:
            sequence_numbers (int array): array of sequence numbers from the MotionSense HRV
            accel_data (float array):     array of accelerometer readings from the MotionSense HRV
            gyro_data (float array):      array of gyroscope readings from the MotionSense HRV
            ppg_data (float array):       array of PPG readings from the MotionSense HRV
            Fs:                           sampling frequency
            window_size:                  length of the data you are passing(has to be 65 secs)
            acceptable_fraction:          the least fraction of expected data

        Returns:
            yhat:                           stress predictions(a probability)

        """
        # TODO: replace code below with implementation
        # Get combined data
        if raw_data is not None:
            if len(raw_data) < acceptable_fraction * window_size * Fs:
                print('Not enough data collected')
                return None
            raw_data = np.insert(raw_data,1,0,axis=1)
            X = self.get_combined_data_from_raw(raw_data)
        else:
            if len(sequence_numbers) < acceptable_fraction * window_size * Fs:
                print('Not enough data collected')
                return None
            X = self.get_combined_data(sequence_numbers, accel_data, gyro_data, ppg_data)
        print(len(X))
        if X.shape[0] < acceptable_fraction * window_size * Fs / 2:
            print('abcPlease wear the watch properly-not enough acceptable data')
            return None

        # return heart rate
        heart_rate = self.get_heart_rate(X)

        # print(heart_rate)
        if heart_rate is None or len(heart_rate) < 10:
            print("Number of heart rate datapoints in the window is below the threshold for predicting stress")
            print('Please wear the watch properly-not enough acceptable data')
            return None
        heart_rate_dict_list = [{"t":key, "hrt":60000/value} for (key, value) in zip(list(heart_rate[:,0]),
                                                                                         list(heart_rate[:,1]))]
        print(heart_rate_dict_list)

        # return feature row
        feature = self.compute_feature_matrix(heart_rate)
        # make and return predictions
        yhat = self.model.predict(feature)[0]  # this is the stress prediction
        feature = feature.reshape(-1)
        hrvs = {self.names[i]:list(feature)[i] for i in range(len(list(feature)))}
        # print(hrvs)
        f = open('predictions.txt','a')
        f.write(yhat)
        f.write('\n')
        f.close()
        return {"predictions":yhat,"heart_rates":heart_rate_dict_list,"features":hrvs}

    def get_heart_rate(self, combined_data):
        from cerebralcortex.algorithms.stress_from_raw_ppg.util import get_feature_matrix
        feature_matrix = get_feature_matrix(combined_data)
        return feature_matrix

    def compute_feature_matrix(self, heart_rate):
        from cerebralcortex.algorithms.stress_from_raw_ppg.ecg import ecg_feature_computation
        self.running_mean.append(np.array([heart_rate[0, 0], np.mean(heart_rate[:, 1])]))
        self.running_std.append(np.array([heart_rate[0, 0], np.std(heart_rate[:, 1])]))
        ms = self.get_mean_std()
        heart_rate[:, 1] = (heart_rate[:, 1] - ms[0]) / np.mean(ms[1])
        feature_matrix = np.array(ecg_feature_computation(heart_rate[:, 0], heart_rate[:, 1])).reshape(-1, 11)
        return feature_matrix

    def get_mean_std(self):
        temp_mean = np.array(self.running_mean)
        temp_std = np.array(self.running_std)
        return [np.mean(temp_mean[np.where(temp_mean[:, 0] > temp_mean[-1, 0] - self.look_back)[0], 1]),
                np.mean(temp_std[np.where(temp_std[:, 0] > temp_std[-1, 0] - self.look_back)[0], 1])]

    def get_combined_data(self, sequence_numbers, accel_data, gyro_data, ppg_data):
        """Prepare data for prediction"""

        # TODO: replace the following with any necessary preprocessing calls:

        # import preprocessing functions from modules in the data directory
        from cerebralcortex.algorithms.stress_from_raw_ppg.util import filter, return_combined_data
        combined_data = return_combined_data(sequence_numbers, [accel_data,gyro_data,ppg_data])
        combined_filtered_data = filter(combined_data)
        return combined_filtered_data
    def get_combined_data_from_raw(self,raw_data):
        from cerebralcortex.algorithms.stress_from_raw_ppg.util import filter, get_decoded_matrix
        combined_data = get_decoded_matrix(raw_data)
        combined_filtered_data = filter(combined_data)
        return combined_filtered_data


class StressModel_RF:
    """
    StressModel wraps a stress prediction model and associated preprocessing code.
    When the class is initialized, it will load the model to be wrapped from the data directory.
    The model can be loaded either 1) from a pickle file or 2) from a model class -- see examples
    in __init__().

    The goal is to encapsulate the preprocessing and prediction code behind a fixed interface.

    See EXAMPLE CODE section at the bottom of the file for usage.
    """

    def __init__(self, data_folder_path):
        """
        Load models here so they don't need to be loaded from scratch each time predict()
        is called.

        Args:
            data_folder_path (string): path to folder containing stored models, processing files, etc.

        """
        # add data directory to path for importing model classes or preprocessing functions
        sys.path.append(data_folder_path)

        # TODO: import model (see examples below)
        self.running_mean = []
        self.running_std = []
        self.look_back = 6 * 60 * 60 * 1000 # 6 hours time to look back (in ms)
        # EXAMPLE: load a pickled model from the data directory like this:
        model_file = 'ppg_clf.p'
        self.model = pickle.load(open(os.path.join(data_folder_path, model_file), 'rb'))
        self.names = ['var','vlf','lf','lf-hf','mean','iqr','hr','80th','20th',
                      'am_median','am_std','am_80th','am_20th',
                      'pa_median','pa_std','pa_80th','pa_20th']

    def predict(self,
                raw_data = None,
                sequence_numbers=None,
                accel_data=None,
                gyro_data=None,
                ppg_data=None,
                acceptable_fraction=.6,
                Fs=25,
                window_size=60):
        """
        Take a window's worth of data, perform any necessary processing on it, and return a
        prediction.  Note that raw location data are not being transmitted from the device,
        so location readings are booleans (True means the user is home)

        Args:
            sequence_numbers (int array): array of sequence numbers from the MotionSense HRV
            accel_data (float array):     array of accelerometer readings from the MotionSense HRV
            gyro_data (float array):      array of gyroscope readings from the MotionSense HRV
            ppg_data (float array):       array of PPG readings from the MotionSense HRV
            Fs:                           sampling frequency
            window_size:                  length of the data you are passing(has to be 65 secs)
            acceptable_fraction:          the least fraction of expected data

        Returns:
            yhat:                           stress predictions(a probability)
              -1:                         Not enough data collected
              -2:                         Watch not worn properly-not enough acceptable data
              -3:                         Number of heart rate datapoints in the window is below the threshold for predicting stress

        """
        # TODO: replace code below with implementation
        # Get combined data
        if raw_data is not None:
            #print(raw_data)
            #print(len(raw_data),acceptable_fraction * window_size * Fs)
            if len(raw_data) < acceptable_fraction * window_size * Fs:
                #print('Not enough data collected')
                return -1
            #print(len(raw_data))
            raw_data = np.insert(raw_data,1,0,axis=1)
            raw_data[:,0] = raw_data[:,0]*1000
            #print(raw_data[:,0])
            X = self.get_combined_data_from_raw(raw_data)
        else:
            if len(sequence_numbers) < acceptable_fraction * window_size * Fs:
                #print('Not enough data collected')
                return -1
            X = self.get_combined_data(sequence_numbers, accel_data, gyro_data, ppg_data)
        if X.shape[0] < acceptable_fraction * window_size * Fs / 2:
            #print(len(X))
            #print('Please wear the watch properly-not enough acceptable data')
            return -2

        # return heart rate
        heart_rate = self.get_heart_rate(X)

        # print(heart_rate)
        if heart_rate is None or len(heart_rate) < 10:
            #print("Number of heart rate datapoints in the window is below the threshold for predicting stress")
            #print('Please wear the watch properly-not enough acceptable data')
            return -3

        heart_rate_r = self.get_2_sec_ts(heart_rate[:,np.array([0,1])])
        if heart_rate_r is None or len(heart_rate_r) < 10:
            #print("Number of heart rate datapoints in the window is below the threshold for predicting stress")
            #print('Please wear the watch properly-not enough acceptable data')
            return -3

        heart_rate_dict_list = [{"t":key, "hrt":60000/value} for (key, value) in zip(list(heart_rate_r[:,0]),
                                                                                     list(heart_rate_r[:,1]))]

        feature = self.compute_feature_matrix(heart_rate_r,heart_rate)
        #print(heart_rate_dict_list)
        yhat = self.model.predict(feature)[0]  # this is the stress prediction

        feature = feature.reshape(-1)
        hrvs = {self.names[i]:list(feature)[i] for i in range(len(list(feature)))}

        return {"predictions":yhat,"heart_rates":heart_rate_dict_list,"features":hrvs}

    def get_2_sec_ts(self,rr_ppg_int):
        m = np.mean(rr_ppg_int[:,1])
        s = np.std(rr_ppg_int[:,1])
        rr_ppg_int = rr_ppg_int[np.where((rr_ppg_int[:,1]>=m-3*s)&(rr_ppg_int[:,1]<=m+3*s))[0],:]
        ts_array = np.arange(rr_ppg_int[0,0],rr_ppg_int[-1,0],1000)
        rr_interval = np.zeros((0,2))
        for t in ts_array:
            index = np.where((rr_ppg_int[:,0]>=t-4000)&(rr_ppg_int[:,0]<=t+4000))[0]
            if len(index) < 2:
                continue
            rr_interval = np.concatenate((rr_interval,np.array([t,np.mean(rr_ppg_int[index,1])]).reshape(-1,2)))
        return rr_interval

    def get_heart_rate(self, combined_data):
        from cerebralcortex.algorithms.stress_from_raw_ppg.util import get_feature_matrix_rf
        feature_matrix = get_feature_matrix_rf(combined_data)
        return feature_matrix

    def compute_feature_matrix(self, heart_rate_r,heart_rate):
        from cerebralcortex.algorithms.stress_from_raw_ppg.ecg import ecg_feature_computation
        self.running_mean.append(np.array([heart_rate_r[0, 0], np.mean(heart_rate_r[:, 1])]))
        self.running_std.append(np.array([heart_rate_r[0, 0], np.std(heart_rate_r[:, 1])]))
        ms = self.get_mean_std()
        heart_rate_r[:, 1] = (heart_rate_r[:, 1] - ms[0]) / np.mean(ms[1])
        feature_matrix = np.array(ecg_feature_computation(heart_rate_r[:, 0]/1000, heart_rate_r[:, 1]/1000)).reshape(-1, 11)
        feature_matrix = list(feature_matrix[:,np.array([0,1,3,4,5,7,8,9,10])].reshape(-1))
        diff = heart_rate[:,np.array([2])]
        feature_matrix.extend([np.median(diff),np.std(diff),np.percentile(diff,80),np.percentile(diff,20)])
        diff = heart_rate[:,np.array([3])]
        feature_matrix.extend([np.median(diff),np.std(diff),np.percentile(diff,80),np.percentile(diff,20)])
        return np.array(feature_matrix).reshape(-1,17)

    def get_mean_std(self):
        temp_mean = np.array(self.running_mean)
        temp_std = np.array(self.running_std)
        return [np.mean(temp_mean[np.where(temp_mean[:, 0] > temp_mean[-1, 0] - self.look_back)[0], 1]),
                np.mean(temp_std[np.where(temp_std[:, 0] > temp_std[-1, 0] - self.look_back)[0], 1])]

    def get_combined_data(self, sequence_numbers, accel_data, gyro_data, ppg_data):
        """Prepare data for prediction"""

        # TODO: replace the following with any necessary preprocessing calls:

        # import preprocessing functions from modules in the data directory
        from cerebralcortex.algorithms.stress_from_raw_ppg.util import filter, return_combined_data
        combined_data = return_combined_data(sequence_numbers, [accel_data,gyro_data,ppg_data])
        combined_filtered_data = filter(combined_data)
        return combined_filtered_data


    def get_combined_data_from_raw(self,raw_data):
        from cerebralcortex.algorithms.stress_from_raw_ppg.util import filter, get_decoded_matrix
        combined_data = get_decoded_matrix(raw_data)
        combined_filtered_data = filter(combined_data)
        #print('-'*10,len(combined_data),len(combined_filtered_data),'-'*10)
        return combined_filtered_data



"""
EXAMPLE CODE
"""


def examples():
    """
    Initializing and using AvailabilityModel
    
    """
    # path to data folder
    data_path = 'data'

    # wrapped availability model
    av_model = AvailabilityModel(data_path)

    # window of data (sequence numbers, accelerometer data, Google Activity API labels, home booleans)
    # seq = np.asarray([0, 1, 2, 3])
    # accel_readings = np.asarray([[2.1, 1.8, 5.2]] * 3)
    #here raw_data refers to data from google activity api for 10 mins instance
    activity_labels = pd.read_csv('raw_data',header=None)

    
    home = 1 #1 for home and 0 for not home
    #
    # # predict
    yhat = av_model.predict(activity_labels, home)

    print("availability prediction: {}".format(yhat))
    

    # path to data folder
    data_path = 'data'

    # wrapped availability model
    stress_model = StressModel(data_path)

    # window of data (sequence numbers, accelerometer data, gyroscope data, ppg data)
    seq = np.asarray([0, 1, 2, 3])
    accel_readings = np.asarray([[2.1, 1.8, 5.2]] * 3)
    gyro_readings = np.asarray([[2.1, 1.8, 5.2]] * 3)
    ppg_readings = np.asarray([[2.1, 1.8, 5.2]] * 3)

    # predict
    yhat = stress_model.predict(seq, accel_readings, gyro_readings, ppg_readings)

    print("stress prediction: {}".format(yhat))


if __name__ == "__main__":
    print("running examples")
    examples()

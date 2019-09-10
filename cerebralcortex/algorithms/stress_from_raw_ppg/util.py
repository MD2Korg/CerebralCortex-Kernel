import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler,RobustScaler
from sklearn.neighbors import LocalOutlierFactor
from scipy import signal
from scipy.stats import skew,kurtosis
from scipy import interpolate
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
from cerebralcortex.algorithms.stress_from_raw_ppg.decode import Preprc

def isDatapointsWithinRange(red,infrared,green):
    """

    :param red: ppg channel=red
    :param infrared: ppg channel = infrared
    :param green: ppg channel = green
    :return: boolean condition specifying dc values in window are within range
    """
    red = np.asarray(red, dtype=np.float32)
    infrared = np.asarray(infrared, dtype=np.float32)
    green = np.asarray(green, dtype=np.float32)
    a =  len(np.where((red >= 30000)& (red<=170000))[0]) < .5*25*3
    b = len(np.where((infrared >= 120000)& (infrared<=230000))[0]) < .5*25*3
    c = len(np.where((green >= 500)& (green<=20000))[0]) < .5*25*3
    if a and b and c:
        return False
    return True

def compute_quality(window):
    """

    :param window: a window containing the datapoints in the form of a numpy array (n*4)
    first column is timestamp, second third and fourth are the ppg channels
    :return: an integer reptresenting the status of the window 0= attached, 1 = not attached
    """
    if len(window)==0:
        return 1
    red = window[:,0]
    infrared = window[:,1]
    green = window[:,2]
    if not isDatapointsWithinRange(red,infrared,green):
        return 1
    if np.mean(red) < 5000 and np.mean(infrared) < 5000 and np.mean(green)<500:
        return 1
    if np.mean(red)<np.mean(green) or np.mean(infrared)<np.mean(red):
        return 1
    diff = 20000
    if np.mean(red)>140000:
        diff = 10000
    if np.mean(red) - np.mean(green) < diff or np.mean(infrared) - np.mean(red)<diff:
        return 1
    if np.var(red) <1 and np.var(infrared) <1 and np.var(green)<1:
        return 1
    return 0
def get_clean_ppg(data):
    """

    :param data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
    ppg green, acl x,y,z, gyro x,y,z
    :return:
    """
    start_ts = data[0,0]
    final_data = np.zeros((0,10))
    ind = np.array([1,2,3])
    while start_ts < data[-1,0]:
        index = np.where((data[:,0]>=start_ts)&(data[:,0]<start_ts+3000))[0]
        temp_data = data[index,:]
        temp_data = temp_data[:,ind]
        if compute_quality(temp_data)==0:
            final_data = np.concatenate((final_data,data[index,:]))
        start_ts = start_ts + 3000
    return final_data

def preProcessing(data,Fs=25,fil_type='ppg'):
    '''
    Inputs
    data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
    ppg green, acl x,y,z, gyro x,y,z
    Fs: sampling rate
    fil_type: ppg or ecg
    Output X2: preprocessed signal data
    preprocessing the data by filtering

    '''
    if data.shape[0]<165:
        return np.zeros((0,data.shape[1]))

    X0 = data[:,1:4]
    X1 = signal.detrend(X0,axis=0,type='constant')
    # if fil_type in ['ppg']:
    b = signal.firls(65,np.array([0,0.3, 0.4, 2 ,2.5,Fs/2]),np.array([0, 0 ,1 ,1 ,0, 0]),
                     np.array([100*0.02,0.02,0.02]),fs=Fs)
    X2 = np.zeros((np.shape(X1)[0]-len(b)+1,data.shape[1]))
    for i in range(X2.shape[1]):
        if i in [1,2,3]:
            box_pts = 5
            box = np.ones(box_pts)/box_pts
            X2[:,i] = signal.convolve(X1[:,i-1],b,mode='valid')
            X2[:,i] = np.convolve(X2[:,i], box, mode='same')
        else:
            X2[:,i] = data[64:,i]

    return X2



def get_filtered_data(data):
    """

    data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
    ppg green, acl x,y,z, gyro x,y,z
    :return: bandpass filtered data of the time when motionsense was attached to wrist
    """
    data = get_clean_ppg(data)
    final_data = preProcessing(data)
    return final_data


def filter(data:pd.DataFrame):
    """

    data: a numpy array of shape n*22 ..this is the raw byte array from the raw stream
    :return: bandpass filtered data of the time when motionsense was attached to wrist
    """
    ind_acl = np.array([10,7,8,9,1,2,3,4,5,6])
    data = data[:,ind_acl]
    #print(data[0])
    final_data = get_filtered_data(data)
    return final_data

def return_interpolated(x,ts):
    interp = interpolate.interp1d(x[:,0],x[:,1:],axis=0,fill_value='extrapolate')
    y = np.zeros((len(ts),x.shape[1]))
    y[:,0] = ts
    y[:,1:] = interp(ts).reshape(-1,x.shape[1]-1)
    return y

def return_combined_data(seq:np.ndarray,data_arr1:list):
    ts_array = seq[:,0]
    data_arr1[0] = return_interpolated(data_arr1[0],ts_array)[:,1:]
    data_arr1[1] = return_interpolated(data_arr1[1],ts_array)[:,1:]
    data_arr1[2] = return_interpolated(data_arr1[2],ts_array)[:,1:]
    data_arr1 = np.concatenate(data_arr1,axis=1)
    timestamps = seq[:,0]
    seq = seq[:,1]
    d = np.diff(seq)
    idx1 = np.where(d < -(1023 - 50))[0]
    idx1 = np.append(idx1, len(seq) - 1)
    for i in range(len(idx1) - 1):
        seq[idx1[i] + 1:idx1[i + 1] + 1] = seq[idx1[i] + 1:idx1[i + 1] + 1] - (i + 1) * d[idx1[i]]
    seq = (seq - seq[0]).astype(int).reshape((len(seq)))
    seq_max = max(seq)  # just some heuristic to make ECG  seq value 4 times
    arr1 = np.concatenate([seq.reshape((len(seq), 1)), data_arr1], axis=1)
    df1 = pd.DataFrame(arr1, columns=['Seq', 'AccX', 'AccY', 'AccZ', 'GyroX',
                                          'GyroY', 'GyroZ', 'LED1', 'LED2', 'LED3'])
    df1.drop_duplicates(subset=['Seq'], inplace=True)

    df2 = pd.DataFrame(np.array(range(seq_max + 1)), columns=['Seq'])

    itime = timestamps[0]
    ftime = timestamps[-1]
    df3 = df2.merge(df1, how='left', on=['Seq'])
    df3['time'] = pd.to_datetime(np.linspace(itime, ftime, len(df2)), unit='ms')
    df3.set_index('time', inplace=True)
    df3.interpolate(method='time', axis=0, inplace=True)  # filling missing data
    df3.dropna(inplace=True)
    df3['time_stamps'] = np.linspace(itime, ftime, len(df2))
    # print(df3)
    return df3.values

def get_decoded_matrix(data: np.ndarray, row_length=22):
    """
    given the raw byte array containing lists it returns the decoded values
    :param row_length:
    :param data: input matrix(*,22) containing raw bytes
    :return: a matrix each row of which contains consecutively sequence
    number,acclx,accly,acclz,gyrox,gyroy,gyroz,red,infrared,green leds,
    timestamp
    """
    if len(data)<1:
        return np.zeros((0,10))
    ts = data[:,0]
    sample = np.zeros((len(ts), row_length))
    sample = data
    ts_temp = np.array([0] + list(np.diff(ts)))
    ind = np.where(ts_temp > 1000)[0]
    initial = 0
    sample_final = [0] * int(row_length / 2)
    for k in ind:
        sample_temp = Preprc(raw_data=sample[initial:k, :])
        initial = k
        if not list(sample_temp):
            continue
        sample_final = np.vstack((sample_final, sample_temp.values))
        # print(sample_final.shape)
    sample_temp = Preprc(raw_data=sample[initial:, :])
    if np.shape(sample_temp)[0] > 0:
        sample_final = np.vstack((sample_final, sample_temp.values))
    if np.shape(sample_final)[0] == 1:
        return np.zeros((0,10))
    return sample_final[1:,:]

def get_pv(ppg_window_val,p=0,Fs=25,peak_percentile=30,peak_distance=.3):
    peak_loc, peak_dict = find_peaks(ppg_window_val[:,p],
                                     distance=Fs*peak_distance,
                                     height=np.percentile(ppg_window_val[:,p],peak_percentile))
    peak_indicator = [1 for o in range(len(peak_loc))]
    valley_loc, valley_dict = find_peaks(-1*ppg_window_val[:,p],
                                         distance=Fs*peak_distance,
                                         height=np.percentile(-1*ppg_window_val[:,p],peak_percentile))
    valley_indicator = [0 for o in range(len(valley_loc))]
    indicator = peak_indicator + valley_indicator
    locs = list(peak_loc) + list(valley_loc)
    heights = list(peak_dict['peak_heights']) + list(valley_dict['peak_heights'])
    channel = [p for o in range(len(locs))]
    peak_valley = np.concatenate((np.array(locs).reshape(-1,1),
                                  np.array(heights).reshape(-1,1),
                                  np.array(indicator).reshape(-1,1),
                                  np.array(channel).reshape(-1,1))
                                 ,axis=1)
    peak_valley = peak_valley[peak_valley[:,0].argsort()]
    lets_see = [np.array([o,o+1,o+2]) for o in range(0,len(locs)-3,1)]
    return peak_valley,lets_see

def entropy(labels, base=None):
    hist1 = np.histogram(labels, bins=20, density=True)
    data = hist1[0]
    ent = -(data*np.log(np.abs(data))).sum()
    return ent
def get_feature_for_channel(ppg_window_val,
                            ppg_window_time,
                            window_size=10,
                            Fs=25,
                            var_threshold=.001,
                            kurtosis_threshold_high=1,
                            kurtosis_threshold_low=-2,
                            skew_threshold_low=-3,
                            skew_threshold_high=3,
                            iqr_diff=.2):
    try:
        ts_array = np.linspace(ppg_window_time[0],ppg_window_time[-1],window_size*Fs)
        interp = interpolate.interp1d(ppg_window_time,ppg_window_val,axis=0,fill_value='extrapolate')
        final_data_2 = interp(ts_array)
        final_data_2 = MinMaxScaler().fit_transform(RobustScaler().fit_transform(final_data_2))
        X = final_data_2.T
        gg = X.T
        predicted = np.array([0]*gg.shape[1])
        for i in range(len(predicted)):
            if len(np.where(np.diff(ppg_window_val[:,i])==0)[0])/ppg_window_val.shape[0] > .2:
                predicted[i] = 1
            if np.var(ppg_window_val[:,i])<var_threshold:
                predicted[i] = 1
            # if not -3<entropy(gg[:,i])<3:
            #     predicted[i] = 1
            if kurtosis(gg[:,i])>kurtosis_threshold_high:
                predicted[i] = 1
            if kurtosis(gg[:,i])<kurtosis_threshold_low:
                predicted[i] = 1
            if not skew_threshold_low<skew(gg[:,i])<skew_threshold_high:
                predicted[i] = 1
            if np.percentile(gg[:,i],75)-np.percentile(gg[:,i],25)<iqr_diff:
                predicted[i] = 1
            if not 0<=len(np.where(np.diff(np.signbit(gg[:,i])))[0])/len(gg[:,i])<=.2:
                predicted[i] = 1
        return predicted,gg,ts_array
    except Exception as e:
        return np.array([1]*ppg_window_val.shape[1]),np.zeros((250,0)),ppg_window_time

def get_feature_peak_valley(ppg_window_val,ppg_window_time,Fs=25,window_size=10):
    feature_for_channel,ppg_window_val,ppg_window_time = get_feature_for_channel(ppg_window_val,
                                                                                 ppg_window_time,
                                                                                 Fs=Fs,
                                                                                 window_size=window_size)
    ppg_window_val = ppg_window_val[:,np.where(feature_for_channel==0)[0]]
    feature_final = np.zeros((0,6))
    if ppg_window_val.shape[1]==0:
        return feature_final
    if ppg_window_val.shape[1]>1:
        height_var = []
        for i in range(ppg_window_val.shape[1]):
            peak_loc, peak_dict = find_peaks(ppg_window_val[:,i], distance=Fs*.3,
                                             height=np.percentile(ppg_window_val[:,i],30))
            height_var.append(np.std(list(peak_dict['peak_heights'])))
        ppg_window_val = ppg_window_val[:,np.argmin(np.array(height_var))].reshape(-1,1)
    for p in range(ppg_window_val.shape[1]):
        peak_valley,lets_see = get_pv(ppg_window_val,p,Fs)
        feature = []
        for ind,item in enumerate(lets_see):
            window = peak_valley[item,:]
            if len(np.unique(window[:,2]))==1 or sum(window[:,2]) not in [2,1] or \
                    len(np.unique(np.abs(np.diff(window[:,2]))))>1:
                continue
            start = np.int64(window[0,0])
            end = np.int64(window[-1,0]) + 1
            if window[1,2] == 0:
                cycle = ppg_window_val[start:end,p]*(-1)
            else:
                cycle = ppg_window_val[start:end,p]
            if not 300<(window[2,0]-window[0,0])*40<1500:
                continue
            feature.append(np.array([ppg_window_time[np.int64(window[1,0])],
                                     np.trapz(cycle),
                                     np.std(window[:,1]),
                                     np.mean(window[:,1]),
                                     window[2,0]-window[0,0],
                                     p]))
        feature = np.array(feature)
        if len(feature)==0:
            continue
        feature_final = np.concatenate((feature_final,feature))
    return feature_final

def get_features_for_kuality(acl):
    f = []
    f.extend(list(np.var(acl[:,2:5],axis=0)))
    return f


def get_data_out(ppg_data,acl_data,
                 Fs=25,
                 window_size=10,
                 step_size=2000,
                 acl_threshold=0.042924592358051586):
    left_data =ppg_data
    acl_l = acl_data*2/16384
    ts_array = np.arange(left_data[0,0],left_data[-1,0],step_size)
    y = []
    for k in range(0,len(ts_array),1):
        t = ts_array[k]
        index_ppg = np.where((left_data[:,0]>=t-window_size*1000/2)&(left_data[:,0]<=t+window_size*1000/2))[0]
        index_acl = np.where((acl_l[:,0]>=t-window_size*1000/2)&(acl_l[:,0]<=t+window_size*1000/2))[0]
        if len(index_ppg)<.6*window_size*Fs:
            continue
        ppg_window_time = left_data[index_ppg,0]
        ppg_window_val = left_data[index_ppg,1:]
        ff = get_features_for_kuality(acl_l[index_acl,:])
        if np.max(ff)>acl_threshold:
            continue
        ppg_window_val = signal.detrend(ppg_window_val,axis=0)
        feature_final = get_feature_peak_valley(ppg_window_val,ppg_window_time,
                                                Fs=Fs,window_size=window_size)
        if feature_final.shape[0]<3:
            continue
        clf = LocalOutlierFactor(n_neighbors=2,contamination=.2)
        ypred = clf.fit_predict(feature_final[:,1:-1])
        y.append(np.array([t,np.median(feature_final[ypred==1,-2])*40]))
    return np.array(y)


def get_feature_matrix(data):
    ppg_data = data[:,np.array([0,1,2,3])]
    acl_data = data[:,np.array([0,0,4,5,6])]
    heart_rate = get_data_out(ppg_data,acl_data)
    return heart_rate





def get_data_out_rf(left_data,acl_data,Fs=25,
                    window_size=10,
                    step_size=6000,
                    acl_threshold=0.042924592358051586):


    def get_feature_peak_valley_rf(ppg_window_val,ppg_window_time,Fs):


        def get_feature_for_channel_rf(ppg_window_val,ppg_window_time):



            def entropy(labels, base=None):
                hist1 = np.histogram(labels, bins=20, density=True)
                data = hist1[0]
                ent = -(data*np.log(np.abs(data))).sum()
                return ent


            try:
                ts_array = np.linspace(ppg_window_time[0],ppg_window_time[-1],200)
                interp = interpolate.interp1d(ppg_window_time,ppg_window_val,axis=0,fill_value='extrapolate')
                final_data_2 = interp(ts_array)
                final_data_2 = RobustScaler(quantile_range=(5,95)).fit_transform(final_data_2)
                X = final_data_2.T
                gg = X.T
                predicted = np.array([0]*gg.shape[1])
                for i in range(len(predicted)):
                    # if not 0<entropy(gg[:,i])<3:
                    #     predicted[i] = 1
                    if np.percentile(gg[:,i],80)<=.6 and np.percentile(gg[:,i],20)>=.4:
                        predicted[i] = 1
                    if kurtosis(gg[:,i])>1:
                        predicted[i] = 1
                    if kurtosis(gg[:,i])<-2:
                        predicted[i] = 1
                    if not 0<=len(np.where(np.diff(np.signbit(gg[:,i])))[0])/len(gg[:,i])<=.2:
                        predicted[i] = 1
                    if not -3<skew(gg[:,i])<3:
                        predicted[i] = 1
                    if np.percentile(gg[:,i],75)-np.percentile(gg[:,i],25)<.2:
                        predicted[i] = 1
                return predicted,gg,ts_array
            except Exception as e:
                print(e)
                return np.array([1]*ppg_window_val.shape[1]),np.zeros((200,0)),ppg_window_time



        feature_for_channel,ppg_window_val,ppg_window_time = get_feature_for_channel_rf(ppg_window_val,
                                                                                     ppg_window_time)
        ppg_window_val = ppg_window_val[:,np.where(feature_for_channel==0)[0]]
        if ppg_window_val.shape[1]==0:
            return ppg_window_val,ppg_window_time
        if ppg_window_val.shape[1]>1:
            height_var = []
            for i in range(ppg_window_val.shape[1]):
                ppg_window_val[:,i] = ppg_window_val[:,i]+i+1
                peak_loc, peak_dict = find_peaks(ppg_window_val[:,i], distance=Fs*.3,width=2,
                                                 height=np.percentile(ppg_window_val[:,i],30))
                height_var.append(np.std(list(peak_dict['widths'])))
            ppg_window_val = ppg_window_val[:,np.argmin(np.array(height_var))].reshape(-1,1)
        return ppg_window_val,ppg_window_time



    def get_pv_1_rf(ppg_window_val,ppg_window_time,Fs=25):
        peak_loc, peak_dict = find_peaks(ppg_window_val[:,0], distance=Fs*.3,width=2,
                                         height=np.percentile(ppg_window_val[:,0],30))
        if len(peak_loc)<3:
            return np.zeros((0,5))
        peak_indicator = [1 for o in range(len(peak_loc))]
        ppg_window_val2 = MinMaxScaler().fit_transform(-1*ppg_window_val)
        valley_loc, valley_dict = find_peaks(ppg_window_val2[:,0], distance=Fs*.3,width=2,
                                             height=np.percentile(ppg_window_val2[:,0],30))
        valley_indicator = [0 for o in range(len(valley_loc))]
        indicator = peak_indicator + valley_indicator
        locs = list(peak_loc) + list(valley_loc)
        heights = list(np.array(peak_dict['peak_heights'])/max(peak_dict['peak_heights']))
        heights = heights + list(np.array(valley_dict['peak_heights'])/max(valley_dict['peak_heights']+1))
        widths = list(np.array(peak_dict['widths'])/max(peak_dict['widths']))
        widths = widths + list(np.array(valley_dict['widths'])/max(valley_dict['widths']))
        peak_valley = np.concatenate((np.array(locs).reshape(-1,1),
                                      np.array(heights).reshape(-1,1),
                                      np.array(indicator).reshape(-1,1),
                                      np.array(widths).reshape(-1,1)),axis=1)
        peak_valley = peak_valley[peak_valley[:,0].argsort()]
        feature_final = []
        for i in range(0,peak_valley.shape[0]-3,1):
            window = peak_valley[i:i+3,:]
            if len(np.unique(window[:,2]))==1 or sum(window[:,2]) not in [2,1] or len(np.unique(np.abs(np.diff(window[:,2]))))>1:
                continue
            if window[1,2]==0:
                ppg_window_val3 = ppg_window_val2
            else:
                ppg_window_val3 = ppg_window_val
            if not 400<(window[2,0]-window[0,0])*40<2000:
                continue
            if abs(ppg_window_val[np.int64(window[0,0])]-ppg_window_val[np.int64(window[2,0])])> .2:
                continue
            start = np.int64(window[0,0])
            mid = np.int64(window[1,0])
            end = np.int64(window[2,0])
            feature_final.append(np.array([ppg_window_time[start],
                                           max(ppg_window_val3[start:end+1,0])-min(ppg_window_val3[start:end+1,0]),
                                           np.trapz(ppg_window_val3[start:end+1,0]),
                                           (window[2,0]-window[0,0])*40]))
        return np.array(feature_final)

    acl_l = acl_data*2/16384
    ts_array = np.arange(left_data[0,0],left_data[-1,0],step_size)
    feature_final = np.zeros((0,4))
    left_data[:,1:] = RobustScaler(quantile_range=(20,80)).fit_transform(left_data[:,1:])

    # from pykalman import KalmanFilter
    # kf = KalmanFilter(n_dim_state=3, n_dim_obs=1)
    # plt.plot(left_data[:1000,0],kf.em(left_data[:,1],n_iter=2).filter(left_data[:1000,1])[0].reshape(-1,1))
    # plt.plot(left_data[:1000,0],left_data[:1000,1]-10,linestyle='-.')
    # plt.show()
    for k in range(0,len(ts_array),1):
        t = ts_array[k]
        index_ppg = np.where((left_data[:,0]>=t)&(left_data[:,0]<=t+window_size*1000))[0]
        index_acl = np.where((acl_l[:,0]>=t-window_size*1000/2)&(acl_l[:,0]<=t+window_size*1000/2))[0]
        if len(index_ppg)<int(.8*Fs*window_size):
            continue
        ff = get_features_for_kuality(acl_l[index_acl,:])
        if np.max(ff)>acl_threshold:
            continue
        ppg_window_time = left_data[index_ppg,0]
        ppg_window_val = left_data[index_ppg,1:]
        ppg_window_val = signal.detrend(ppg_window_val,axis=0)
        ppg_window_val,ppg_window_time = get_feature_peak_valley_rf(ppg_window_val,ppg_window_time,Fs)
        if ppg_window_val.shape[1]==0:
            continue
        ppg_window_val = MinMaxScaler().fit_transform(ppg_window_val)
        feature_final_1 = get_pv_1_rf(ppg_window_val,ppg_window_time)
        if feature_final_1.shape[0]==0:
            continue
        feature_final_save = feature_final
        feature_final = np.concatenate((feature_final,feature_final_1))
        feature_final_temp = feature_final[np.where((feature_final[:,0]>=feature_final_1[0,0]-10000)&(feature_final[:,0]<=feature_final_1[-1,0]))[0],:]
        if feature_final_temp.shape[0]<5:
            continue
        from pyod.models.knn import KNN
        clf = KNN(n_neighbors=3,contamination=.2)
        temp = PCA(n_components=1).fit_transform(feature_final_temp[:,1:])
        ypred2 = clf.fit(temp).predict(temp)
        feature_final_temp = feature_final_temp[ypred2==0,:]
        feature_final_temp = feature_final_temp[np.where(feature_final_temp[:,0]>=feature_final_1[0,0])[0],:]
        feature_final = np.concatenate((feature_final_save,feature_final_temp))
        feature_final = np.unique(feature_final,axis=0)
    return feature_final


def get_feature_matrix_rf(data):
    ppg_data = data[:,np.array([0,1,2,3])]
    acl_data = data[:,np.array([0,0,4,5,6])]
    # from sklearn.decomposition import FastICA
    # transformer = FastICA(n_components=2,
    #                       random_state=0)
    # X_transformed = transformer.fit_transform(ppg_data[:,1:])
    # # ppg_data[:,1] = X_transformed[:,0]
    # # ppg_data[:,2] = X_transformed[:,0]
    # # ppg_data[:,3] = X_transformed[:,0]
    #
    plt.plot(ppg_data[:,0],ppg_data[:,1:])
    plt.show()
    # plt.plot(left_data[:,0],left_data[:,1])
    # plt.show()
    heart_rate = get_data_out_rf(ppg_data,acl_data)

    return heart_rate[:,np.array([0,3,1,2])]

# -*- encoding: utf-8 -*-

# This is the map process for prediction

import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append('./')

from keras.models import model_from_json
import numpy as np

import unzip_ts_features as unzip

def mapper(line, meta, model, snap_mean, snap_std, ts_mean, ts_std):
    # user_id \t begin_date \t end_date \t snap features ... \t ts_features_json
    arr = line.strip('\n').split('\t')
    user_id, end_date, snap_feature, ts_json = arr[0], arr[2], arr[3: -1], arr[-1]

    snap_feature = np.asarray(snap_feature).astype('float32').reshape((1, -1))
    snap_feature -= snap_mean
    snap_feature /= snap_std

    ts_feature = unzip.unzip_record(end_date + '\t' + ts_json, meta)
    if ts_feature is None:
        features, months, channels = meta.shape
        ts_feature = np.zeros(shape=(months, features, channels))
    ts_feature = ts_feature.astype('float32')
    #ts_feature -= ts_mean
    #ts_feature /= ts_std
    tensor_shape = ts_feature.shape
    ts_feature = ts_feature.reshape([ 1, tensor_shape[0], tensor_shape[1] ])


    prob = model.predict([ snap_feature, ts_feature ])[0, 1]

    print "{}\t{}".format(user_id, prob)


if __name__ == "__main__":
    meta = unzip.parse_feature_map("./feature_map.json")
    json_file = open("./fusion_net_final.json", 'r')
    model_json = json_file.read()
    json_file.close()
    model = model_from_json(model_json)
    model.load_weights("./fusion_net_final.h5")

    snap_mean = np.load("./fusion_snap_mean_final.npy")
    snap_std = np.load("./fusion_snap_std_final.npy") + 0.000000001

    ts_mean = np.load("./fusion_ts_mean_final.npy")
    ts_std = np.load("./fusion_ts_std_final.npy") + 0.000000001

    for line in sys.stdin:
        mapper(line, meta, model, snap_mean, snap_std, ts_mean, ts_std)
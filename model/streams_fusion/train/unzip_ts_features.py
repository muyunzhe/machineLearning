# -*- encoding: utf-8 -*-

import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from collections import OrderedDict
import numpy as np
import pandas as pd
import json
import datetime

np.set_printoptions(suppress=True)

"""
@Parameters: 
    file_to_path: configuration file path
@Returns: 
    3D matrix with the shape (features, months, channels),
          each item contains the feature index, zero means padding.
"""
def parse_feature_map(file_to_path):
    try:
        config = json.load(open(file_to_path), object_pairs_hook=OrderedDict)
    except IOError:
        print >> sys.stderr, "Error: Configuration path error"
        return None
    except ValueError:
        print >> sys.stderr, "Error: Json format error"
        return None

    channels = config["channels"]
    months = config["months"]
    features = config["features"]

    meta_tensor = np.zeros(shape=(features, months, channels), dtype=np.int)

    for channel_index in range(channels):
        feature_indexes = []
        key = "channel{0}".format(channel_index + 1)
        feature_index_list = config[key]
        for feature_index_item in feature_index_list:
            # Attention: [start, end]
            start, end = map(int, feature_index_item.split('-'))
            feature_indexes.extend(range(start, end + 1))
        shuffled_indexes = \
            np.broadcast_to(np.asarray(feature_indexes, dtype=str), shape=(months, len(feature_indexes))).transpose()

        meta_tensor[: len(feature_indexes), : months, channel_index] = shuffled_indexes

    return meta_tensor

"""
@Parameters:
    record: end_date(1970-01-01) \t features_json OR end_month(1970-01) \t features_json
        features_json format: 
            {
                "2017-01": {
                    "1001": 32.0,
                    "1099": 64.0,
                    ......
                },
                "2017-02": {
                    "2001": 128.0,
                    ......
                },
                ......
            }

    meta_tensor: 3D matrix with the shape (months, features, channels),
        each item contains the feature index, zero means padding.

@Returns:
    3D data Volume(months, features, channels)
"""
def unzip_record(record, meta_tensor):
    def time_format(input, format):
        try:
            t = datetime.datetime.strptime(input, format)
        except ValueError:
            return None
        return t
        
    record = record.strip('\n')
    arr = record.split("\t")
    
    if len(arr) != 2:
        print >> sys.stderr, "Error: your record should be end_date \t features_json OR end_month \t features_json, \
                              not {0}".format(record)
        return None
    
    end_time, features_json = arr
    end_month = None
    end_month = time_format(end_time, "%Y-%m-%d").strftime("%Y-%m") or time_format(end_time, "%Y-%m").strftime("%Y-%m")

    try:
        flatten_features = json.loads(features_json, object_pairs_hook=OrderedDict)
    except:
        print >> sys.stderr, "Warning: No events for this record"
        return None

    features, months, channels = meta_tensor.shape

    # Attention: [begin_month, end_month)
    end = pd.Period(end_month)
    begin = end - months
    month_series = map(str, pd.period_range(begin, end, freq='M')[:-1])

    if len(month_series) != months:
        print >> sys.stderr, "Error: month series length should be {0},".format(months) + \
            "not {0}".format(len(month_series))
        return None

    data_tensor = np.zeros(shape=(months, features, channels))

    for month_index, month in enumerate(month_series):
        if month not in flatten_features:
            continue
        month_info = flatten_features[month]
        month_data = np.zeros(shape=(features, channels))
        meta_info = meta_tensor[:, month_index, :]
        for i in range(channels):
            for j in range(features):
                month_data[j, i] = month_info.get(str(meta_info[j, i]), 0)
        data_tensor[month_index, :, :] = month_data

    return data_tensor

if __name__ == "__main__":
    # just for test
    conf = sys.argv[1]
    data_file = sys.argv[2]

    meta = parse_feature_map(conf)

    dataset_x = []
    dataset_y = []

    line_count = 0
    with open(data_file, "r") as f:
        for line in f:
            line_count += 1
            print line_count
            label, data = unzip_record(line, meta)
            dataset_y.append(label)
            dataset_x.append(data)
    dataset_y = np.asarray(dataset_y)
    dataset_x = np.asarray(dataset_x)
    print "labels length: ", len(dataset_y)
    print "samples length: ", len(dataset_x)

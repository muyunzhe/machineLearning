# -*- encoding: utf-8 -*-

import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import json
import time
import multiprocessing as mp
from functools import partial
import pickle

import numpy as np
import pandas as pd
import sklearn
from sklearn import metrics

import keras
from keras.models import load_model

import unzip_ts_features as unzip
import resnet_module as resnet
import model_evaluate as eva

def load_data(fm_conf):
    start_time = time.time()
    print "start to load feature map meta"
    meta = unzip.parse_feature_map(fm_conf)
    print "complete to load meta, which costs %f seconds" % (time.time() - start_time)

    # user_id \t begin_date \t end_date \t label \t snap features ... \t ts_features_json
    start_time = time.time()
    print "start to load data"
    data = pd.read_table("./testset_order_old", header=None).as_matrix()
    print "complete to load data, which costs %f seconds," % (time.time() - start_time)
    samples_count = data.shape[0]
    print "number of samples ", samples_count

    test_labels = data[:, 3].reshape(-1).astype(int)
    test_snap_features = data[:, 4:-1].astype("float32")
    ts_features_json = data[:, 2] + '\t' + data[:, -1]

    # unzip time series features using parallel computing
    pool = mp.Pool(processes=None)
    start_time = time.time()
    print "start to unzip time series features"
    # map ensures result ordering
    test_ts_features = np.asarray(pool.map(partial(unzip.unzip_record, meta_tensor=meta), ts_features_json)).astype("float32")
    print "complete to unzip data, which costs %f seconds" % (time.time() - start_time)

    # load the snap mean & std
    mean = pickle.load(open("./fusion_snap_mean_final", "r"))
    std = pickle.load(open("./fusion_snap_std_final", "r"))

    test_snap_features -= mean
    test_snap_features /= std

    # load the ts mean & std
    mean = pickle.load(open("./fusion_ts_mean_final", "r"))
    std = pickle.load(open("./fusion_ts_std_final", "r"))

    test_ts_features -= mean
    test_ts_features /= std

    print "At last,\n"
    print "test label shape ", test_labels.shape
    print "test snap features shape ", test_snap_features.shape
    print "test ts features shape ", test_ts_features.shape

    return test_labels, test_snap_features, test_ts_features

def test():
    #test_labels, test_snap_features, test_ts_features = load_data("./feature_map.json")

    #test_ts_features = test_ts_features.reshape([ test_ts_features.shape[0], test_ts_features.shape[1], test_ts_features.shape[2] ])

    model = load_model("./fusion_net_final.hdf5")
    
    probs = model.predict([ test_snap_features, test_ts_features ])[:, 1].reshape(-1)
    facts = test_labels
    
    auc, ks, opt, acc, prec, recall = eva.model_evaluate(facts, probs)
    print "\nThe test set metrics are as follows:\n"
    print "auc is: %.5f\n" % auc
    print "ks is: %.5f\n" % ks
    print "opt cut is: %.5f\n" % opt
    print "accuracy is: %.5f\n" % acc
    print "precision is: %.5f\n" % prec
    print "recall is: %.5f\n" % recall

    print "*" * 80

    print "\nThe segments metrics are as follows:\n"
    print eva.evaluate_by_segments(facts, probs)
    
    print "-" * 80
    print "-" * 80

    print "predict zero number: %d" % sum(probs == 0.0)
    print "predict one number: %d" % sum(probs == 1.0)
    remain_indexes = np.logical_and(probs > 0.0, probs < 1.0)
    
    new_facts = facts[remain_indexes]
    new_probs = probs[remain_indexes]

    print "After removing predictions of ones and zeros......"
    
    auc, ks, opt, acc, prec, recall = eva.model_evaluate(new_facts, new_probs)
    print "\nThe test set metrics are as follows:\n"
    print "auc is: %.5f\n" % auc
    print "ks is: %.5f\n" % ks
    print "opt cut is: %.5f\n" % opt
    print "accuracy is: %.5f\n" % acc
    print "precision is: %.5f\n" % prec
    print "recall is: %.5f\n" % recall

    print "*" * 80
    
    print eva.evaluate_by_segments(new_facts, new_probs)    

if __name__ == "__main__":
    test()
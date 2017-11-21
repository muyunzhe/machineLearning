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
import keras.backend as K
from keras.layers.core import Dense, Dropout, Activation
from keras.layers import Input, Conv1D, MaxPooling1D, GlobalAveragePooling1D
from keras.layers import LSTM
from keras.layers.normalization import BatchNormalization
from keras.layers.advanced_activations import PReLU
from keras.models import Model
from keras.optimizers import Adam

import unzip_ts_features as unzip
import resnet_module as resnet
import model_evaluate as eva
import plot_evaluate as plot

class AUC_Callback(keras.callbacks.Callback):
    def on_train_begin(self, logs={}):
        self.losses = []

    def on_epoch_end(self, epoch, logs={}):
        self.losses.append(logs.get("val_loss"))
        y_pred = self.model.predict([ self.validation_data[0], self.validation_data[1] ])[:, 1].reshape(-1)
        auc, ks, opt, acc, prec, recall = eva.model_evaluate(self.validation_data[2][:, 1].reshape(-1).astype(int), y_pred)
        print "\n\nAt Epoch %d, the current metrics information as follows:\nauc is: %.5f\nks is: %.5f\nopt cut \
        is: %.5f\naccuracy is: %.5f\nprecision is: %.5f\nrecall is: %.5f\n\n" % (epoch + 1, auc, ks,
                opt, acc, prec, recall)
        plot.plot_metrics(self.validation_data[2][:, 1].reshape(-1).astype(int), y_pred)


def load_data(fm_conf):
    start_time = time.time()
    print "start to load feature map meta"
    meta = unzip.parse_feature_map(fm_conf)
    print "complete to load meta, which costs %f seconds" % (time.time() - start_time)

    # user_id \t begin_date \t end_date \t label \t snap features ... \t ts_features_json
    start_time = time.time()
    print "start to load data"
    data = pd.read_table("./samples60000", header=None).as_matrix()
    print "complete to load data, which costs %f seconds," % (time.time() - start_time)
    samples_count = data.shape[0]
    print "number of samples ", samples_count

    labels = data[:, 3].reshape(-1).astype(int)
    snap_features = data[:, 4:-1].astype("float32")
    ts_features_json = data[:, 2] + '\t' + data[:, -1]

    # unzip time series features using parallel computing
    pool = mp.Pool(processes=None)
    start_time = time.time()
    print "start to unzip time series features"
    # map ensures result ordering
    ts_features = np.asarray(pool.map(partial(unzip.unzip_record, meta_tensor=meta), ts_features_json)).astype("float32")
    print "complete to unzip data, which costs %f seconds" % (time.time() - start_time)

    # let's go shuffling and split train/valid data
    shuffled_indexes = np.arange(samples_count)
    np.random.shuffle(shuffled_indexes)

    labels = labels[shuffled_indexes]
    snap_features = snap_features[shuffled_indexes]
    ts_features = ts_features[shuffled_indexes]

    train_valid_split_ratio = 0.9

    good_labels = labels[labels == 0]
    bad_labels =  labels[labels == 1]
    good_snap_features = snap_features[labels == 0]
    bad_snap_features = snap_features[labels == 1]
    good_ts_features = ts_features[labels == 0]
    bad_ts_features = ts_features[labels == 1]

    good_count = len(good_labels)
    bad_count = len(bad_labels)

    print "We have %d good samples, and %d bad samples." % (good_count, bad_count)

    train_labels = np.hstack([
                              good_labels[: int(good_count * train_valid_split_ratio)],
                              bad_labels[: int(bad_count * train_valid_split_ratio)]
                              ])
    valid_labels = np.hstack([
                              good_labels[int(good_count * train_valid_split_ratio): ],
                              bad_labels[int(bad_count * train_valid_split_ratio): ]
                              ])

    train_snap_features = np.vstack([
                                       good_snap_features[: int(good_count * train_valid_split_ratio)],
                                       bad_snap_features[: int(bad_count * train_valid_split_ratio)]
                                    ])
    valid_snap_features = np.vstack([
                                       good_snap_features[int(good_count * train_valid_split_ratio): ],
                                       bad_snap_features[int(bad_count * train_valid_split_ratio): ]
                                     ])

    train_ts_features = np.vstack([
                                     good_ts_features[: int(good_count * train_valid_split_ratio)],
                                     bad_ts_features[: int(bad_count * train_valid_split_ratio)]
                                  ])
    valid_ts_features = np.vstack([
                                     good_ts_features[int(good_count * train_valid_split_ratio): ],
                                     bad_ts_features[int(bad_count * train_valid_split_ratio): ]
                                  ])

    train_samples = len(train_labels)
    valid_sample = len(valid_labels)

    shuffled_indexes = np.arange(train_samples)
    np.random.shuffle(shuffled_indexes)
    train_labels = train_labels[shuffled_indexes]
    train_snap_features = train_snap_features[shuffled_indexes]
    train_ts_features = train_ts_features[shuffled_indexes]

    # let's do normalization
    mean = np.mean(train_snap_features, axis=0)
    std = np.std(train_snap_features, axis=0) + 0.000000001

    # save the snap mean & std
    pickle.dump(mean, open("./fusion_snap_mean", "w"))
    pickle.dump(std, open("./fusion_snap_std", "w"))

    train_snap_features -= mean
    train_snap_features /= std
    valid_snap_features -= mean
    valid_snap_features /= std

    mean = np.mean(train_ts_features, axis=0)
    std = np.std(train_ts_features, axis=0) + 0.000000001

    # save the ts mean & std
    pickle.dump(mean, open("./fusion_ts_mean", "w"))
    pickle.dump(std, open("./fusion_ts_std", "w"))

    train_ts_features -= mean
    train_ts_features /= std
    valid_ts_features -= mean
    valid_ts_features /= std

    print "At last,\n"
    print "train label shape ", train_labels.shape
    print "train snap features shape ", train_snap_features.shape
    print "train ts features shape ", train_ts_features.shape

    print "valid label shape ", valid_labels.shape
    print "valid snap features shape ", valid_snap_features.shape
    print "valid ts features shape ", valid_ts_features.shape

    return train_labels, train_snap_features, train_ts_features, \
           valid_labels, valid_snap_features, valid_ts_features

def mlp_cnn_fusion_model(snaps, ts):
    # residual stream
    res_x = resnet.ResNet.build_resnet(snaps, 512, 512)

    # conv stream
    ts_x = Conv1D(512, 3, kernel_regularizer=keras.regularizers.l2(0.01), kernel_initializer="glorot_normal")(ts)
    ts_x = PReLU()(ts_x)
    ts_x = MaxPooling1D(2)(ts_x)
    ts_x = Conv1D(512, 3, kernel_regularizer=keras.regularizers.l2(0.01), kernel_initializer="glorot_normal")(ts_x)
    ts_x = PReLU()(ts_x)
    ts_x = GlobalAveragePooling1D()(ts_x)
    ts_x = Dropout(0.6)(ts_x)

    # fusion
    fusion_x = keras.layers.add([res_x, ts_x])
    fusion_x = BatchNormalization()(fusion_x)
    fusion_x = PReLU()(fusion_x)
    fusion_x = Dropout(0.6)(fusion_x)  
    
    return fusion_x

def mlp_lstm_fusion_model(snaps, ts):
    # residual stream
    res_x = resnet.ResNet.build_resnet(snaps, 512, 512)

    # lstm stream
    ts_x = LSTM(512, kernel_regularizer=keras.regularizers.l2(0.01), kernel_initializer="glorot_normal")(ts) 

    # fusion
    fusion_x = keras.layers.add([res_x, ts_x])
    fusion_x = BatchNormalization()(fusion_x)
    fusion_x = PReLU()(fusion_x)
    fusion_x = Dropout(0.6)(fusion_x)
    
    return fusion_x

def mlp_cnn_lstm_fusion_model(snaps, ts):
    # residual stream
    res_x = resnet.ResNet.build_resnet(snaps, 512, 512)
    
    # conv stream
    conv_x = Conv1D(512, 3, kernel_regularizer=keras.regularizers.l2(0.01), kernel_initializer="glorot_normal")(ts)
    conv_x = PReLU()(conv_x)
    conv_x = MaxPooling1D(2)(conv_x)
    conv_x = Conv1D(512, 3, kernel_regularizer=keras.regularizers.l2(0.01), kernel_initializer="glorot_normal")(conv_x)
    conv_x = PReLU()(conv_x)
    conv_x = GlobalAveragePooling1D()(conv_x)
    conv_x = Dropout(0.6)(conv_x)
    
    # lstm stream
    ts_x = LSTM(512, kernel_regularizer=keras.regularizers.l2(0.01), kernel_initializer="glorot_normal")(ts)
    
    # fusion
    fusion_x = keras.layers.add([res_x, conv_x, ts_x])
    fusion_x = BatchNormalization()(fusion_x)
    fusion_x = PReLU()(fusion_x)
    fusion_x = Dropout(0.6)(fusion_x)
    
    return fusion_x    

def train():
    train_labels, train_snap_features, train_ts_features, \
    valid_labels, valid_snap_features, valid_ts_features = load_data("./feature_map.json")

    train_labels = keras.utils.to_categorical(train_labels, num_classes=2)
    valid_labels = keras.utils.to_categorical(valid_labels, num_classes=2)

    train_ts_features = train_ts_features.reshape([ train_ts_features.shape[0], train_ts_features.shape[1], train_ts_features.shape[2] ])
    valid_ts_features = valid_ts_features.reshape([ valid_ts_features.shape[0], valid_ts_features.shape[1], train_ts_features.shape[2] ])

    snaps = Input(shape=(2632,), name="snap_input")
    ts = Input(shape=(16, 834), name="ts_input")

    fusion = mlp_cnn_fusion_model(snaps, ts)
    predict = Dense(2, activation="softmax")(fusion)

    final_model = Model(inputs=[snaps, ts], outputs=[predict])
    
    print final_model.summary()

    final_model.compile(Adam(beta_1=0.9, beta_2=0.999, lr=0.001, decay=0.01), loss='categorical_crossentropy', metrics=['accuracy'])

    auc = AUC_Callback()
    tb = keras.callbacks.TensorBoard("./Graph")
    lr_decay = keras.callbacks.ReduceLROnPlateau(factor=0.1, patience=10, verbose=1)
    checkpoint = keras.callbacks.ModelCheckpoint(filepath="./fusion_net.hdf5", save_best_only=True)
    
    final_model.fit([train_snap_features, train_ts_features], [train_labels], 
        batch_size=256, epochs=5000, callbacks=[auc, tb, lr_decay, checkpoint],
        validation_data=([valid_snap_features, valid_ts_features], valid_labels), shuffle=True)
    

if __name__ == "__main__":
    train()
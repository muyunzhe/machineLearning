# -*- encoding: utf-8 -*-

import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import pandas as pd
import numpy as np
import sklearn
from sklearn import metrics

def model_evaluate(truth, pred):
    """
    Parameters:
        truth: a list of ground truth, 1 or 0
        pred: a list of guess probabilities, range [0, 1]
    Returns:
        auc, ks, optimal cut point, accuracy, precision and recall
    Raises:
        ValueError if both lengths of inputs are not equal
    """
    if len(truth) != len(pred):
        raise ValueError("Lengths of truth and guesst must be equal!")

    truth = np.asarray(truth).astype(int)
    pred = np.asarray(pred).astype(float)

    valid_indexes = np.logical_or(truth == 0, truth == 1)
    truth = truth[valid_indexes]
    pred = pred[valid_indexes]

    # auc
    fpr, tpr, thresholds = metrics.roc_curve(truth, pred, pos_label=1)
    auc = metrics.auc(fpr, tpr)

    # ks & optimal cut point
    data = pd.DataFrame({"pos": truth, "pred": pred})
    data["neg"] = 1 - data["pos"]
    data["bucket"] = pd.cut(data["pred"], 1000)
    grouped = data.groupby('bucket', as_index=False)

    agg1 = pd.DataFrame()
    agg1["min_pred"] = grouped.min()["pred"]
    agg1["max_pred"] = grouped.max()["pred"]
    agg1["pos_num"] = grouped.sum()["pos"]
    agg1["neg_num"] = grouped.sum()["neg"]
    agg1["total"] = agg1["pos_num"] + agg1["neg_num"]

    agg2 = (agg1.sort_values(by='min_pred')).reset_index(drop=True)
    agg2["ks"] = np.abs((agg2["neg_num"] * 1.0 / agg2["neg_num"].sum()).cumsum() - \
            (agg2["pos_num"] * 1.0 / agg2["pos_num"].sum()).cumsum()) * 100

    ks = agg2["ks"].max()
    opt_index = agg2["ks"].argmax()
    opt_min, opt_max = agg2["min_pred"].iloc[opt_index], agg2["max_pred"].iloc[opt_index]
    opt_cut = (opt_min + opt_max) / 2.0

    # accuracy, precision & recall
    tp = np.sum(np.logical_and(pred > opt_cut, truth == 1))
    fp = np.sum(np.logical_and(pred > opt_cut, truth == 0))
    fn = np.sum(np.logical_and(pred < opt_cut, truth == 1))
    tn = np.sum(np.logical_and(pred < opt_cut, truth == 0))

    accuracy = (tp + tn) * 1.0 / (tp + fp + fn + tn)
    precision = tp * 1.0 / (tp + fp + 0.000001)
    recall = tp * 1.0 / (tp + fn + 0.0000001)

    return auc, ks, opt_cut, accuracy, precision, recall

def evaluate_by_segments(truth, pred, buckets=20):
    """
    Parameters:
        truth: a list of ground truth, 1 or 0, neither 1 nor 0 means non-performance sample
        pred: a list of guess probabilities, range [0, 1]
        buckets: number of equal lengths of buckets cut in [0, 1]
    Returns:
        buckets segments performance, fields as follows:

        score_range \t total_samples \t total samples ratio \t has_perf_samples \t
        has_perf_samples_ratio \t neg_samples \t pos_samples \t precision \t recall

        which score ranges are [0, 0.05], (0.05, 0.1], (0.1, 0.15], ..., (0.95, 1] if buckets=20 
    Raises:
        ValueError if both lengths of inputs are not equal
    """

    if len(truth) != len(pred):
        raise ValueError("Lengths of truth and guesst must be equal!")

    truth = np.asarray(truth).astype(int)
    pred = np.asarray(pred).astype(float)
    sentinel = np.asarray(["non"] * len(truth))

    # add sentinels of 0 and 1
    truth = np.append(truth, -999)
    pred = np.append(pred, 0)
    sentinel = np.append(sentinel, "sentinel")

    truth = np.append(truth, -999)
    pred = np.append(pred, 1)
    sentinel = np.append(sentinel, "sentinel")

    data = pd.DataFrame({"pos": truth, "pred": pred}, index=sentinel)
    data["neg"] = 1 - data["pos"]
    data.loc[data["pos"] != 1, "pos"] = 0
    data.loc[data["neg"] != 1, "neg"] = 0
    data["bucket"] = pd.cut(data["pred"], buckets)
    data.drop("sentinel", inplace=True)

    grouped = data.groupby("bucket", as_index=False)

    agg = pd.DataFrame()
    agg["bucket"] = grouped.apply(lambda x: x.name)
    agg["total_samples"] = grouped.count()["pred"]
    agg["pos_samples"] = grouped.sum()["pos"]
    agg["neg_samples"] = grouped.sum()["neg"]
    agg["has_perf_samples"] = agg["pos_samples"] + agg["neg_samples"]

    agg["total_samples_ratio"] = agg["total_samples"] * 1.0 / agg["total_samples"].sum()
    agg["has_perf_samples_ratio"] = agg["has_perf_samples"] * 1.0 / agg["has_perf_samples"].sum()
    agg["precision"] = agg["pos_samples"] * 1.0 / agg["has_perf_samples"]
    agg["recall"] = agg["pos_samples"] * 1.0 / agg["pos_samples"].sum()

    agg = agg.sort_values(by="bucket").reset_index(drop=True)
    agg = agg.fillna(0)
    agg["pos_samples"] = agg["pos_samples"].astype(int)
    agg["neg_samples"] = agg["neg_samples"].astype(int)
    agg["has_perf_samples"] = agg["has_perf_samples"].astype(int)

    return agg

if __name__ == "__main__":
    preds = np.random.random(10000)
    truths = np.random.randint(2, size=10000)

    print model_evaluate(truths, preds)

    preds = np.random.random(10000)
    truths = np.random.randint(2, size=10000)
    print evaluate_by_segments(truths, preds)

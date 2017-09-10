# coding=utf-8
import platform

import matplotlib; matplotlib.use('Agg')
import matplotlib.pylab as plt
import numpy as np
import os

from datetime import datetime
from numpy import sum, logical_and, sqrt
from sklearn.metrics import roc_curve, auc
from sklearn.utils.validation import column_or_1d

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.figsize'] = 16, 9


def logging(*params):
    print datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ' '.join(['%s' for _ in params]) % params


def workspace():
    """
        获取工作目录
    returns:
            工作目录路径
    raises:
        assert: 无工作目录
    """
    if platform.dist()[0] == 'debian':
        ws = './'

    # ws = os.environ.get('DATA_HOME')
    assert ws is not None, 'Please set environment variable: DATA_HOME for data storage.'

    if not os.path.exists(ws):
        os.makedirs(ws, 0755)
    return ws


def cumulation(true_label, guess_label):
    """ 计算样本分布累积占比及对应的KS值 """
    cumulative_1 = plt.hist(guess_label[true_label == 1], bins=np.arange(0, 1, 0.001), color='blue', normed=1, cumulative=1, histtype='step', label='Bad users')
    cumulative_2 = plt.hist(guess_label[true_label == 0], bins=np.arange(0, 1, 0.001), color='green', normed=1, cumulative=1, histtype='step', label='Good users')
    return cumulative_1, cumulative_2, np.abs(cumulative_1[0] - cumulative_2[0])


def evaluate(true_label, guess_label, hardCut=False):
    """
        模型性能统计分析
    Args:
        true_label: 测试样本真实标签序列
        guess_label: 测试样本预测标签序列
    returns:
        (aucv, precision, recall, accuracy, fscore, ks, actual_cut)
    """
    true_label = column_or_1d(true_label)
    guess_label = column_or_1d(guess_label)
    
    cumulative_1, _, cumu_delta = cumulation(true_label, guess_label)
    ks = np.max(cumu_delta)
    softcut = cumulative_1[1][np.argmax(cumu_delta)]
    
    if isinstance(hardCut, float):
        actual_cut = hardCut
    else:
        hardCut = 0.5
        actual_cut = softcut
    
    fpr, tpr, _ = roc_curve(true_label, guess_label)
    A = sum(logical_and(guess_label >= actual_cut, true_label == 1))
    B = sum(logical_and(guess_label >= actual_cut, true_label == 0))
    C = sum(logical_and(guess_label < actual_cut, true_label == 1))
    D = sum(logical_and(guess_label < actual_cut, true_label == 0))
    
    accuracy = 1.0 * (A + D) / (A + B + C + D)
    precision = 1.0 * A / (A + B)
    acc_pos = 1.0 * A / (A + C)
    acc_neg = 1.0 * D / (B + D)
    recall = acc_pos
    gmean = sqrt(acc_pos * acc_neg)
    fscore = 2.0 * precision * recall / (precision + recall)
    aucv = auc(fpr, tpr)
    logging(u'实际类别为1的个数: %d, 判定类别为1的个数: %d' % (sum(true_label == 1), sum(guess_label >= actual_cut)))
    logging(u'实际类别为0的个数: %d, 判定类别为0的个数: %d' % (sum(true_label == 0), sum(guess_label < actual_cut)))
    logging(u'A=%d, B=%d, C=%d, D=%d' % (A, B, C, D))
    logging(u'Precision=%.4f, Recall=%.4f, Accuracy=%.4f' % (precision, recall, accuracy))
    logging(u'AUC:%.4f, G-mean=%.4f, F-score=%.4f' % (aucv, gmean, fscore))
    logging('KS=%.4f,' % ks, 'Softcut=%.4f,' % softcut, 'HardCut=%.4f' % hardCut)
    
    return (aucv, precision, recall, accuracy, fscore, ks, actual_cut)


def visualization(true_label, guess_label):
    """
        可视化统计分析
    Args:
        true_label: 测试样本真实标签序列
        guess_label: 测试样本预测标签序列
    returns:
        None
    """
    
    plt.clf()
    plt.gcf().set_size_inches(16, 9)    
    
    # 整体预判概率分布
    plt.subplot(2, 2, 1)
    plt.hist(guess_label, bins=50, color='green', weights=np.ones_like(guess_label) / len(guess_label))
    plt.grid()
    plt.xlabel(u'预测概率')
    plt.ylabel(u'用户占比')    
    plt.title(u'整体预判概率分布')
           
    # ROC曲线
    fpr, tpr, _ = roc_curve(true_label, guess_label)
    plt.subplot(2, 2, 2)
    plt.plot(fpr, tpr, label='ROC Curve')
    plt.plot([0, 1], [0, 1], 'y--')
    plt.grid()
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.title(u'ROC曲线')
           
    # 正负类别概率分布
    plt.subplot(2, 2, 3)
    plt.hist(guess_label[true_label == 1], bins=50, color='blue',
               weights=np.ones_like(guess_label[true_label == 1]) / len(guess_label[true_label == 1]), label='Bad users')
    plt.hist(guess_label[true_label == 0], bins=50, color='green', alpha=0.8,
               weights=np.ones_like(guess_label[true_label == 0]) / len(guess_label[true_label == 0]), label='Good users')
    plt.grid()
    plt.xlabel(u'预测概率')
    plt.ylabel(u'用户占比')
    plt.title(u'正负类别概率分布')
    plt.legend(loc='best')
           
    # 概率累积分布
    plt.subplot(2, 2, 4)
    cumulative_1, _, cumu_delta = cumulation(true_label, guess_label)
    plt.plot(cumulative_1[1][1:], cumu_delta, color='red', label='KS Curve')
    plt.grid()
    plt.title(u'概率累积分布')
    plt.xlabel(u'预测概率')
    plt.ylabel(u'累积占比')
    plt.legend(loc='upper left')
    plt.savefig(workspace() + '/evaluate.png', dpi=100)
    


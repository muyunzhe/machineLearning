# coding=utf-8

import cPickle
import json
import os
import time
from codecs import open
from collections import OrderedDict
from datetime import datetime
from math import log
from optparse import OptionParser

import numpy as np
import pandas as pd
from numpy import logical_and
from scipy.stats.stats import pearsonr
from sklearn.utils import shuffle

from information_value import WOE
from utils import logging, workspace


def pearson_ccs(dataset, iv_dict, ratio):
    """
    找出所有高于ratio的自相关的特征集，从中保留iv最大的一个，其余加入remove_columns
    :param dataset: (N,M)
    :param iv_dict: key:特征名称 value:信息量
    :param ratio: 阈值
    :return: remove_columns
    """
    header = dataset.columns
    remove_columns = []
    for i in xrange(dataset.shape[1] - 1):
        if header[i] not in iv_dict:
            continue
        high_corrs = {header[i]: iv_dict[header[i]]}
        for j in xrange(i + 1, dataset.shape[1]):
            if header[j] not in iv_dict:
                continue
            pr = pearsonr(dataset[header[i]], dataset[header[j]])[0]
            if abs(pr) >= ratio:
                high_corrs[header[j]] = iv_dict[header[j]]
                print 'pr:%.4f\t%s\t%s' % (pr, header[i], header[j])
        if len(high_corrs) > 1:
            max_iv = max(high_corrs.items(), key=lambda x: x[1])
            high_corrs.pop(max_iv[0])
            remove_columns.extend(high_corrs.keys())
    return list(set(remove_columns))


def categorizing(original, intervals):
    """
    为每个离散区间计算离散特征值
    :param original: 原始值
    :param intervals: 某个特征被划分出来的多个区间
    :return: 原始值所属的特征区间的序号
    """
    if original < intervals[0][0]:
        return 1
    elif original >= intervals[-1][1]:
        return len(intervals)

    for i, v in enumerate(intervals):
        if v[0] <= original < v[1]:
            return i + 1

    raise Exception('{} is out of these intervals: {}'.format(original, intervals))


def entropy(pos_values, neg_values):
    counts = [pos_values.count(), neg_values.count()]
    pdf = counts / np.sum(counts).astype(np.float)
    return -np.sum(np.ma.filled(pdf * np.log2(pdf), fill_value=0))


class discretization():
    """
    Desc: 对连续特征，自动分割成五组，基于熵的离散化

    Warning: 为了加快计算，对特征做了整数转换，在使用改接口之前，确保特征值被整数话后有意义，比如把占比乘以100。
    """

    def __init__(self, basename, tablename, sample_columns, nbin=5, entropylimit=0.05, examplelimit=0.01,
                 prosess_type='woe', feature_selection=True, ratio=0.9):
        """
        discretization对象初始化
        :param basename: 保存文件的名称
        :param tablename: 保存sql语句的表名称
        :param sample_columns: 不需要做转换的列名称，必须在所有转换列之前且连续
        :param nbin: 划分区间数量
        :param entropylimit: 最小区间信息熵值占比
        :param examplelimit: 最小区间样本个数占比
        :param prosess_type: 转换类型：woe or dis
        :param feature_selection: 是否需要pearsonr特征筛选
        :param ratio: feature_selection为True时生效，pearsonr相关性筛选的阈值
        """
        self.nbin = nbin
        self.entropylimit = entropylimit  # 最小区间信息熵值占比
        self.examplelimit = examplelimit  # 最小区间样本个数占比

        self.tablename = tablename
        self.sample_columns = sample_columns
        self.prosess_type = prosess_type
        self.feature_selection = feature_selection
        self.ratio = ratio

        self.fn_raw_test = '{}_raw.test'.format(basename)
        self.fn_raw_train = '{}_raw.train'.format(basename)
        self.fn_dis_test = '{}_dis.test'.format(basename)
        self.fn_dis_train = '{}_dis.train'.format(basename)
        self.fn_woe_test = '{}_woe.test'.format(basename)
        self.fn_woe_train = '{}_woe.train'.format(basename)
        self.fn_dis_rm_test = '{}_dis_rm.test'.format(basename)
        self.fn_dis_rm_train = '{}_dis_rm.train'.format(basename)
        self.fn_woe_rm_test = '{}_woe_rm.test'.format(basename)
        self.fn_woe_rm_train = '{}_woe_rm.train'.format(basename)
        self.fn_ivs_dict = '{}_ivs.dict'.format(basename)
        self.fn_woes_dict = '{}_woes.dict'.format(basename)
        self.fn_intervals_dict = '{}_intervals.dict'.format(basename)
        self.fn_dis_hql = '{}_dis.hql'.format(basename)
        self.fn_woe_hql = '{}_woe.hql'.format(basename)

        dirname = os.path.dirname(os.path.realpath(basename))
        if not os.path.isdir(dirname):
            os.mkdir(dirname)

        self.selected_columns = {}
        self.dis_rm_columns = {}
        self.woe_rm_columns = {}
        self.iv_dict = {}
        self.woes_dict = OrderedDict()
        self.intervals_dict = OrderedDict()

    def _segment(self, features, labels):
        """
        对所有特征的值域进行分段
        :param features: (N,M)
        :param labels: (N,1)
        :return: 经过排序的区间信息熵字典: key--区间, value--信息熵
        """
        assert isinstance(features, pd.core.series.Series), 'Features is not the instance of Series.'
        assert isinstance(labels, pd.core.series.Series), 'Labels is not the instance of Series.'
        assert features.shape[0] == labels.shape[0], 'The dimensions of features and label are unequal.'

        self.nfeature = features.shape[0]
        self.mincount = features.shape[0] * self.examplelimit  # 最小区间样本数至少为样本总数的self.examplelimit
        self.pos_data = features[labels == 1].sort_values()
        self.neg_data = features[labels == 0].sort_values()
        sum_entropy = entropy(self.pos_data, self.neg_data)
        min_entorpy = sum_entropy * self.entropylimit
        # 区间信息熵字典: key--区间, value--信息熵;
        self.seg_ents = {(features.min(), features.max() + 1): sum_entropy}
        while len(self.seg_ents) < self.nbin:
            maxent = max(self.seg_ents.items(), key=lambda x: x[1])
            if maxent[1] < min_entorpy: break
            interval = maxent[0]  # 对熵值最大区间继续分割
            pos_parts = self.pos_data[logical_and(self.pos_data >= interval[0], self.pos_data < interval[1])]
            neg_parts = self.neg_data[logical_and(self.neg_data >= interval[0], self.neg_data < interval[1])]
            # 如果该区间内某类别的特征值少于1个，没有必要再对该区间进行分裂.
            if pos_parts.unique().shape[0] <= 1 or neg_parts.unique().shape[0] <= 1:
                self.seg_ents[interval] -= 1.0
            else:
                self._split_intervals(pos_parts, neg_parts, interval)
        logging(sorted(self.seg_ents.iteritems(), key=lambda d: d[0]))

        return sorted(self.seg_ents.keys())

    def _split_intervals(self, pos_parts, neg_parts, interval):
        """
        @param interval: 特征值的上下限，不是真正的区间边界， 半开区间.
        """
        sub_data = pd.concat([pos_parts, neg_parts])
        uniques = sorted(sub_data.unique())
        cut_info = (np.inf, uniques[0], 0., 0.)
        for cut in uniques[1:]:
            lpd, lnd = pos_parts[pos_parts < cut], neg_parts[neg_parts < cut]
            rpd, rnd = pos_parts[pos_parts >= cut], neg_parts[neg_parts >= cut]
            lcount = float(lpd.shape[0] + lnd.shape[0])
            rcount = float(rpd.shape[0] + rnd.shape[0])
            min_count = min(lpd.shape[0], lnd.shape[0], rpd.shape[0], rnd.shape[0])

            if min_count == 0 or lcount < self.mincount or rcount < self.mincount:
                continue

            left_ent = lcount / self.nfeature * entropy(lpd, lnd)
            right_ent = rcount / self.nfeature * entropy(rpd, rnd)
            loc_ent = left_ent + right_ent
            if loc_ent < cut_info[0]:
                cut_info = (loc_ent, cut, left_ent, right_ent)

        if cut_info[1] == uniques[0]:
            self.seg_ents[interval] -= 1.0
        else:
            self.seg_ents.pop(interval)
            self.seg_ents[(interval[0], cut_info[1])] = round(cut_info[2], 4)
            self.seg_ents[(cut_info[1], interval[1])] = round(cut_info[3], 4)

    def fit(self, X, y=None):
        """
        1. 逐个特征基于熵值离散化成nbin个区间 
        2. 计算计算特征woe及iv值
        3. 保存转换后的离散特征和woe特征
        4. 保存特征值的iv及离散区间
        :param X: (N,M)
        :param y: (N,1)
        :return: 
        """
        # dataset = pd.read_table(self.fn_raw_train, sep=',', header=0)
        # labels = dataset.pop(self.labelname)
        X.reset_index(drop=True, inplace=True)
        y.reset_index(drop=True, inplace=True)
        user_info = X.loc[:, self.sample_columns]
        logging('user_info:{}'.format(user_info.columns))
        dataset = X.drop(user_info, axis=1).fillna(0).astype(np.int32)
        labels = y
        logging('Begin to discretize features', dataset.shape)
        start = time.clock()

        woe = WOE()
        woe_arr = []
        to_drop = []
        for column in dataset.columns:
            desc = dataset[column].describe(percentiles=[0.98])
            minv, maxv = max(-1, desc['min']), round(desc['98%'])
            features = dataset[column].clip(minv, maxv)
            n_uniques = features.nunique()
            if n_uniques < 2 or desc['std'] < 0.05:
                to_drop.append(column)
            else:
                if n_uniques < 1000:
                    feature_values = features.apply(lambda x: int(x))
                    seg_ents_keys_sorted = self._segment(feature_values, labels)
                else:
                    # 对数化、离散化： 拉伸低频，压缩高频，系数补偿、饱和特性、平滑低频段的震荡。
                    feature_values = features.apply(lambda x: int(log(x - minv + 0.1 ** 8, 1.01)))
                    seg_ents_keys_sorted = [
                        (round(1.01 ** seg_ents_keys_sorted[0]) + minv, round(1.01 ** seg_ents_keys_sorted[1]) + minv)
                        for seg_ents_keys_sorted in self._segment(feature_values, labels)]
                seg_index = features.apply(categorizing, args=(seg_ents_keys_sorted,))
                woe_dict, iv = woe.woe_single_x(seg_index, labels)
                logging(
                    '{}({}, {}), iv: {}, intervals: {}'.format(column, minv, maxv, round(iv, 4), seg_ents_keys_sorted))
                assert len(seg_ents_keys_sorted) == len(woe_dict), '{} ---- {}'.format(seg_ents_keys_sorted, woe_dict)
                if iv <= 0.02:
                    to_drop.append(column)
                else:
                    woe_arr.append(woe_dict)
                    self.iv_dict[column], self.woes_dict[column] = iv, woe_dict
                    dataset.loc[:, column], self.intervals_dict[column] = seg_index, seg_ents_keys_sorted
        if to_drop:
            dataset.drop(to_drop, axis=1, inplace=True)

        logging('End to discretize features', dataset.shape)

        self.selected_columns = dataset.columns

        # dis特征处理
        # temp_dataset = woe.woe_replace(dataset, np.array(woe_arr))
        # woe_dataset = pd.DataFrame(X, columns=self.selected_columns)
        # dis特征存储
        dis_dataset = pd.concat([user_info, dataset], axis=1)
        dis_dataset.insert(dis_dataset.shape[1], labels.name, labels)
        dis_dataset.to_csv(self.fn_dis_train, index=False)
        self._create_dis_hql(self.tablename)
        logging('End to story dis features', dis_dataset.shape)
        # woe特征处理
        temp_dataset = woe.woe_replace(dataset, np.array(woe_arr))
        woe_dataset = pd.DataFrame(temp_dataset, columns=self.selected_columns)
        # woe特征存储
        woe_dataset = pd.concat([user_info, woe_dataset], axis=1)
        woe_dataset.insert(woe_dataset.shape[1], labels.name, labels)
        woe_dataset.to_csv(self.fn_woe_train, index=False)
        self._create_woe_hql(self.tablename)
        logging('End to story woe features', woe_dataset.shape)

        cPickle.dump(self.woes_dict, open(self.fn_woes_dict, 'wb'))
        cPickle.dump(self.intervals_dict, open(self.fn_intervals_dict, 'wb'))
        with open(self.fn_ivs_dict, 'w') as fp:
            json.dump(self.iv_dict, fp, encoding='utf-8')

        if self.feature_selection is True:
            self.dis_rm_columns = pearson_ccs(dis_dataset, self.iv_dict, ratio=self.ratio)
            dis_rm_dataset = dis_dataset.drop(self.dis_rm_columns, axis=1)
            # dis_rm_dataset.insert(dis_rm_dataset.shape[1], labels.name, labels)
            # dis_rm_dataset = pd.concat([user_info, dis_rm_dataset], axis=1)
            dis_rm_dataset.to_csv(self.fn_dis_rm_train, index=False)
            logging('dis feature after feature selection', dis_rm_dataset.shape)

            self.woe_rm_columns = pearson_ccs(woe_dataset, self.iv_dict, self.ratio)
            woe_rm_dataset = woe_dataset.drop(self.woe_rm_columns, axis=1)
            # woe_rm_dataset.insert(woe_rm_dataset.shape[1], labels.name, labels)
            # woe_rm_dataset = pd.concat([user_info, woe_rm_dataset], axis=1)
            woe_rm_dataset.to_csv(self.fn_woe_rm_train, index=False)
            logging('woe feature after feature selection', woe_rm_dataset.shape)

        logging('Discretizing features finished.', dataset.shape, 'Time elapsed: %.2f s' % (time.clock() - start))
        return self

    def transform(self, X, y):
        """
        根据fit完的模型对数据集X进行转换，然后附上y返回.

        Parameters
        ----------
        X : (N,M) DataFrame
            Input， 可以包含userinfo，但不包含label
        y : (N,1) DataFrame
            Input， label

        Returns
        -------
        r : (N,) DataFrame
            根据参数中的prosess_type、feature_selection返回不同的结果
            结果中包含了userinfo、features、label
        """
        X.reset_index(drop=True, inplace=True)
        y.reset_index(drop=True, inplace=True)
        """ 使用与训练样本一样的分段方法，对测试样本分段， 离散值为WOE """
        user_info = X.loc[:, self.sample_columns]
        logging('user_info:{}'.format(user_info.columns))
        test_dataset = X.drop(user_info, axis=1).fillna(0).astype(np.int32)
        logging('Begin to discretize features', test_dataset.shape)
        labels = y

        woe_dataset = pd.DataFrame()
        # woe特征转换
        for column in self.selected_columns:
            if column in self.woes_dict and column in self.intervals_dict:
                woes = self.woes_dict[column]
                intervals = self.intervals_dict[column]
                woe = lambda x: woes[categorizing(x, intervals)]
                woe_dataset[column] = test_dataset[column].apply(woe)
            else:
                woe_dataset[column] = test_dataset[column]
        woe_dataset = pd.concat([user_info, woe_dataset], axis=1)
        woe_dataset.insert(woe_dataset.shape[1], labels.name, labels)
        woe_dataset.to_csv(self.fn_woe_test, index=False)
        logging('transform woe features', woe_dataset.shape)
        if self.feature_selection is True:
            woe_rm_dataset = woe_dataset.drop(self.woe_rm_columns, axis=1)
            woe_rm_dataset.to_csv(self.fn_woe_rm_test, index=False)
            logging('transform woe after features selected', woe_rm_dataset.shape)

        # dis特征转换
        dis_dataset = pd.DataFrame()
        for column in self.selected_columns:
            if column in self.intervals_dict:
                intervals = self.intervals_dict[column]

                def discrete(x):
                    return categorizing(x, intervals)

                dis_dataset[column] = test_dataset[column].apply(discrete)
            else:
                dis_dataset[column] = test_dataset[column]
        dis_dataset = pd.concat([user_info, dis_dataset], axis=1)
        dis_dataset.insert(dis_dataset.shape[1], labels.name, labels)
        dis_dataset.to_csv(self.fn_dis_test, index=False)
        logging('transform dis features', woe_dataset.shape)
        if self.feature_selection is True:
            dis_rm_dataset = dis_dataset.drop(self.dis_rm_columns, axis=1)
            dis_rm_dataset.to_csv(self.fn_dis_rm_test, index=False)
            logging('transform dis after features selected', dis_rm_dataset.shape)

        if self.prosess_type == 'woe':
            if self.feature_selection is True:
                return woe_rm_dataset
            else:
                return woe_dataset
        else:
            if self.feature_selection is True:
                return dis_rm_dataset
            else:
                return dis_dataset

    def fit_transform(self, X, y):
        """ 依次调用fit、transform方法 """
        return self.fit(X, y).transform(X, y)

    def _create_dis_hql(self, tablename):
        """
        生成离散特征值的sql。

        Parameters
        ----------
        tablename : String
            数据来源表

        Returns
        -------
        self : 
            保存dis转换时的字典信息，生成dis转换的sql语句
        """
        fields_dict = OrderedDict()

        case_start = 'case\nwhen {} < {} then {}'
        when_then = 'when {} >= {} and {} < {} then {}'
        case_end = 'else {}\nend'

        for column in self.selected_columns:
            if column in self.woes_dict and column in self.intervals_dict:
                intervals = self.intervals_dict[column]
                conditions = [case_start.format(column, intervals[0][1], 1)]
                for index, interval in enumerate(intervals[1:-1]):
                    sql = when_then.format(column, interval[0], column, interval[1], index + 2)
                    conditions.append(sql)
                conditions.append(case_end.format(len(intervals)))
                fields_dict[column] = '\n'.join(conditions)
        fields = ',\n'.join('{} as {}'.format(v, k) for k, v in fields_dict.iteritems())
        with open(self.fn_dis_hql, 'w', encoding='utf-8') as fp:
            fp.write('select \n%s,\n%s\nfrom %s;' % (',\n'.join(self.sample_columns), fields, tablename))

        return self

    def _create_woe_hql(self, tablename):
        """
        生成离散特征值的sql。

        Parameters
        ----------
        tablename : String
            数据来源表

        Returns
        -------
        self : 
            保存woe转换时的字典信息，生成woe转换的sql语句
        """
        fields_dict = OrderedDict()

        case_start = 'case\nwhen {} < {} then {}'
        when_then = 'when {} >= {} and {} < {} then {}'
        case_end = 'else {}\nend'

        for column in self.selected_columns:
            if column in self.woes_dict and column in self.intervals_dict:
                woes = self.woes_dict[column]
                intervals = self.intervals_dict[column]
                assert len(intervals) == len(woes), '{} ---- {}'.format(woes, intervals)
                conditions = [case_start.format(column, intervals[0][1], woes[1])]
                for index, interval in enumerate(intervals[1:-1]):
                    sql = when_then.format(column, interval[0], column, interval[1], woes[index + 2])
                    conditions.append(sql)
                conditions.append(case_end.format(woes[len(intervals)]))
                fields_dict[column] = '\n'.join(conditions)

        fields = ',\n'.join('{} as {}'.format(v, k) for k, v in fields_dict.iteritems())
        with open(self.fn_woe_hql, 'w', encoding='utf-8') as fp:
            fp.write('select \n%s,\n%s\nfrom %s;' % (',\n'.join(self.sample_columns), fields, tablename))

        return self


def split_dataset(dataset, labelname='label', test_ratio=0.3):
    """
    将输入dataset分解为训练集、测试集
    :param dataset: (N,M) DataFrame
    :param labelname: String dataset中label列的名称
    :param test_ratio: 测试集占比
    :return: 训练集数据、训练集label、测试集数据、测试集label
    """
    logging('Dataset: ', dataset.shape)
    """ 调整标签列的位置，方便以后数据切割 """
    labels = dataset.pop(labelname)
    dataset.insert(dataset.shape[1], labelname, labels)

    pos_data = shuffle(dataset[dataset[labelname] == 1])
    neg_data = shuffle(dataset[dataset[labelname] == 0])

    cut = int(pos_data.shape[0] * test_ratio)
    """ 训练集使用与原始数据一致的正负样本比例 """
    n_neg_train = neg_data.shape[0] / pos_data.shape[0] * pos_data.iloc[cut:].shape[0]

    test_data = shuffle(pd.concat((pos_data.iloc[:cut], neg_data.iloc[:cut])))
    train_data = shuffle(pd.concat((pos_data.iloc[cut:], neg_data.iloc[-n_neg_train:])))

    # test_data.to_csv(fn_raw_test, index=False)
    # train_data.to_csv(fn_raw_train, index=False)

    logging('Train set: ', train_data.shape, 'Test set: ', test_data.shape)
    return train_data.iloc[:, :-1], train_data.iloc[:, -1], test_data.iloc[:, :-1], test_data.iloc[:, -1]


if __name__ == '__main__':
    parser = OptionParser(usage="%prog -f filename -t tablename", version="%prog 1")
    parser.add_option('-f', '--filename',
                      dest="filename",
                      default='other_orders',
                      help=u"原始训练特征文件名")
    parser.add_option('-t', '--table',
                      dest="tablename",
                      default='other_orders',
                      help=u"原始预测特征表名")

    (options, args) = parser.parse_args()

    # 读特征文件
    # raw_data = pd.read_table('{0}/{1}'.format(workspace(), options.filename), header=0)
    raw_data = pd.read_table('other_orders.txt', header=0)
    raw_data.rename(columns=lambda x: x.split('.')[1], inplace=True)

    # 指定删除、null值填0
    rm_columns = ['user_id', 'order_time', 'busi_type', 'order_no',
                  'od_days', 'fst_od_days']
    # raw_data = raw_data.drop(rm_columns, axis=1).fillna(0).astype(np.int32)
    # raw_data = raw_data.drop(rm_columns, axis=1).fillna(0).astype(np.int32)

    today = datetime.now().strftime('%Y_%m_%d')

    train_data, train_y, test_data, test_y = split_dataset(raw_data)

    segment = discretization('{0}/{1}/{2}'.format(workspace(), today, options.filename), options.tablename,
                             sample_columns=rm_columns)
    segment.fit(train_data, train_y)

    segment.transform(test_data, test_y)

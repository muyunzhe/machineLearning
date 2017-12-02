#!/bin/env python
#-*- encoding: utf-8 -*-
import numpy
from math import exp
from numpy import mat, shape, ones


def load_data():
    data = []
    label = []
    file_object = open('testSet.txt')
    for line in file_object.readlines():
        arr = line.strip().split()
        data.append([1.0, float(arr[0]), float(arr[1])])
        label.append(int(arr[2]))
    return data, label

def sigmoid(x):
    return 1.0 / (1 + numpy.exp(-x))

def grad_ascent(data, label):
    data_mat = numpy.mat(data)
    label_mat = numpy.mat(label).transpose()
    n, m = data_mat.shape
    alpha = 0.001
    max_step = 500
    weights = numpy.ones((m, 1))
    for i in range(max_step):
        h = sigmoid(data_mat * weights)
        err = (label_mat - h)
        weights = weights + alpha * data_mat.transpose() * err
    return weights


def plogBestFit(wei):
    import matplotlib.pyplot as plt
    weights = wei.getA()
    dataMat,labelMat = load_data()
    dataArr = numpy.array(dataMat)
    n = shape(dataArr)[0]
    xcord1 = []
    ycord1 = []
    xcord2 = []
    ycord2 = []
    for i in range(n):
        if int(labelMat[i]) == 1:
            xcord1.append(dataArr[i, 1]);
            ycord1.append(dataArr[i, 2])
        else:
            xcord2.append(dataArr[i, 1]);
            ycord2.append(dataArr[i, 2])
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.scatter(xcord1, ycord1, s=30, c='red', marker='s')
    ax.scatter(xcord2, ycord2, s=30, c='green')
    x = numpy.arange(-3.0, 3.0, 0.1)
    y = (-weights[0] - weights[1] * x) / weights[2]
    ax.plot(x, y)
    plt.xlabel('X1');
    plt.ylabel('X2');
    plt.show()


if __name__ == '__main__':
    dataArr,labelMat = load_data()
    result = grad_ascent(dataArr,labelMat)
    plogBestFit(result)
    print result
#!/bin/env python
#-*- encoding: utf-8 -*-
from collections import defaultdict
from numpy import *
import operator
import matplotlib
import matplotlib.pyplot as plt


def createDataSet():
    group = array([[1.0,1.1],[1.0,1.0],[0,0],[0,0.1]])
    labels = ['A', 'A', 'B', 'B']
    return group, labels

def classify0(inX, dataSet, labels, k):
    dataSetSize = dataSet.shape[0]
    diffMat = tile(inX, (dataSetSize,1)) - dataSet
    saDiffMat = diffMat**2
    saDistances = saDiffMat.sum(axis=1)
    distance = saDistances**0.5
    sortedDistance = distance.argsort()
    classCount = {}
    for i in range(k):
        voteIlable = labels[sortedDistance[i]]
        classCount[voteIlable] = classCount.get(voteIlable, 0) +1

    sortedClassCount = sorted(classCount.iteritems(),
    key = operator.itemgetter(1), reverse = True)
    return sortedClassCount[0][0]

if __name__ == '__main__':
    group, labels = createDataSet()
    classify0([0,0],group,labels,3)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.scatter(group[:,0], group[:,1])
    print 'a'
import regression

import matplotlib.pyplot as plt
from numpy import *

if __name__ == '__main__':
    xArr ,yArr = regression.loadDataSet('abalone.txt')

    xMat = mat(xArr)
    yMat = mat(yArr)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    # ax.scatter(xMat[:,1].flatten().A[0], yMat.T[:,0].flatten().A[0], s=2, c='red')
    #
    # xCopy = xMat.copy()
    # xCopy.sort(0)
    #ws = regression.standRegres(xArr, yArr)
    # yHat = xCopy * ws
    # ax.plot(xCopy[:,1],yHat)

    # srtInd = xMat[:,1].argsort(0)
    # xSort = xMat[srtInd][:,0,:]
    # yHat = regression.lwlr_test(xArr, xArr, yArr, 0.03)
    # ax.plot(xSort[:,1], yHat[srtInd])

    ridge_weights = regression.ridgeTest(xArr, yArr)
    ax.plot(ridge_weights)


    plt.show()

    #print corrcoef(yHat.T, yMat)
    #print xArr[0:2]
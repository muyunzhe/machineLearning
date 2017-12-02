from numpy import *
from regTrees import *


if __name__ == '__main__':
    my_dat = loadDataSet('ex00.txt')
    my_mat = mat(my_dat)
    createTree(my_dat)
    pass
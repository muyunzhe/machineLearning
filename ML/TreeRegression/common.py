from numpy import *


def load_data_set(file_name):
    data_mat = []
    fr = open(file_name)
    for line in fr.readlines():
        cur_line = line.strip().split('\t')
        flt_line = map(float, cur_line)
        data_mat.append(flt_line)
    return data_mat


def bin_split_data_set(data_set, feature, value):
    mat0 = data_set[nonzero(data_set[:, feature] > value)[0], :][0]
    mat1 = data_set[nonzero(data_set[:, feature] <= value)[0], :][0]
    return mat0, mat1


def reg_leaf(data_set):
    return mean(data_set[:,-1])


def reg_err(data_set):
    return var(data_set[:,-1]) * shape(data_set)[0]


def create_tree(data_set, leaf_type=reg_leaf, err_type=reg_err, ops=(1,4)):
    return


def choose_best_split(data_set, leaf_type=reg_leaf, err_type=reg_err, ops=(1,4)):
    tolS = ops[0]
    tolN = ops[1]
    if len(set(data_set[:,1].T.tolist()[0])) == 1:
        return None, leaf_type(data_set)

    m, n = shape(data_set)
    S = err_type(data_set)
    bestS = inf
    best_index = 0
    best_value = 0
    for feat_index in range(n-1):
        for split_val in set(data_set[:,feat_index]):
            mat0, mat1 = bin_split_data_set(data_set, feat_index, split_val)
            if shape(mat0)[0] < tolN or shape(mat1)[0] < tolN:
                continue
            newS = err_type(mat0) + err_type(mat1)
            if newS < bestS:
                best_index = feat_index
                best_value = split_val
                bestS = newS

    if S - bestS < tolS:
        return None, leaf_type(data_set)

    mat0, mat1 = bin_split_data_set(data_set, best_index, best_value)
    if shape(mat0)[0] < tolN or shape(mat1)[0] < tolS:
        return None, leaf_type(data_set)

    return best_index, best_value


if __name__ == '__main__':
    test_mat = mat(eye(4))
    mat0, mat1 = bin_split_data_set(test_mat, 1, 0.5)
    pass
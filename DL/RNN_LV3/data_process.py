import numpy as np

def Standardize(seq):
    centerized = seq - np.mean(seq, axis = 0)
    normalized = centerized / np.std(centerized, axis = 0)
    return normalized

mfc = np.load('X.npy')
art = np.load('Y.npy')
total_samples = len(mfc)
vali_size = 0.2


def data_prer(X, Y):
    D_input = X[0].shape[1]
    result_data = []
    for x,y in zip(X, Y):
        result_data.append([Standardize(x).reshape((1, -1, D_input)).astype("float32"),
                            Standardize(y).astype("float32")])
    return result_data

data = data_prer(mfc, art)

train = data[int(total_samples * vali_size)]
test = data[:int(total_samples * vali_size)]


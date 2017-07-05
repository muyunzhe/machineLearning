import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

class FNN(object):
    """Build a general FeedForward neural network
        Parameters
        ----------
        learning_rate : float
        drop_out : float
        Layers : list
            The number of layers
        N_hidden : list
            The numbers of nodes in layers
        D_input : int
            Input dimension
        D_label : int
            Label dimension
        Task_type : string
            'regression' or 'classification'
        L2_lambda : float
    """

    def __init__(self, learning_rate, Layers, N_hidden, D_input, D_label, Task_type='regression', L2_lambda=0.0):
        # var
        self.learning_rate = learning_rate
        self.Layers = Layers
        self.N_hidden = N_hidden
        self.D_input = D_input
        self.D_label = D_label
        # 类型控制loss函数的选择
        self.Task_type = Task_type
        # l2 regularization的惩罚强弱，过高会使得输出都拉向0
        self.L2_lambda = L2_lambda
        # 用于存放所累积的每层l2 regularization
        self.l2_penalty = tf.constant(0.0)

        # 用于生成tensorflow缩放图的,括号里起名字
        with tf.name_scope('Input'):
            self.inputs = tf.placeholder(tf.float32, [None, D_input], name="inputs")
        with tf.name_scope('Label'):
            self.labels = tf.placeholder(tf.float32, [None, D_label], name="labels")
        with tf.name_scope('keep_rate'):
            self.drop_keep_rate = tf.placeholder(tf.float32, name="dropout_keep")

        # 初始化的时候直接生成，build方法是后面会建立的
        self.build('F')

    def weight_init(self, shape):
        # shape : list [in_dim, out_dim]
        # 在这里更改初始化方法
        # 方式1：下面的权重初始化若用ReLU激活函数，可以使用带有6个隐藏层的神经网络
        #       若过深，则使用dropout会难以拟合。
        # initial = tf.truncated_normal(shape, stddev=0.1)/ np.sqrt(shape[1])
        # 方式2：下面的权重初始化若用ReLU激活函数，可以扩展到15个隐藏层以上（通常不会用那么多）
        initial = tf.random_uniform(shape, minval=-np.sqrt(5) * np.sqrt(1.0 / shape[0]),
                                    maxval=np.sqrt(5) * np.sqrt(1.0 / shape[0]))
        return tf.Variable(initial)

    def bias_init(self, shape):
        initial = tf.constant(0.1, shape=shape)
        return tf.Variable(initial)



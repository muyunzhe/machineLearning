#!/usr/bin/python
# -*- encoding:utf-8 -*-
from tensorflow.examples.tutorials.mnist import input_data
import tensorflow as tf
import numpy as np
print(tf.__version__)

mnist = input_data.read_data_sets('/home/zhongjuliu/projects/machineLearning/dataset', one_hot=True)

trX, trY, teX, teY = mnist.train.images, mnist.train.labels, mnist.test.images, mnist.test.labels

lr = 0.001
training_iters = 100000
batch_size = 128

n_input = 28
n_step = 28
n_hidden = 128
n_class = 10

x = tf.placeholder(tf.float32, [None, n_step, n_input])
y = tf.placeholder(tf.float32, [None, n_class])

weights = {
    'in':tf.Variable(tf.random_normal([n_input, n_hidden])),
    'out':tf.Variable(tf.random_normal([n_hidden, n_class]))
}

biases = {
    'in':tf.Variable(tf.constant(0.1, shape=[n_hidden, ])),
    'out':tf.Variable(tf.constant(0.1, shape=[n_class, ]))
}

def RNN(X, weights, biases):
    X = tf.reshape(X, [-1, n_input])
    x_in = tf.matmul(X, weights['in']) + biases['in']
    x_in = tf.reshape(x_in, [-1, n_step, n_hidden])
    lstm_cell = tf.contrib.rnn.BasicLSTMCell(n_hidden, forget_bias=1.0)
    init_state = lstm_cell.zero_state(batch_size, dtype=tf.float32)
    outputs, final_state = tf.nn.dynamic_rnn(lstm_cell, x_in, initial_state=init_state, time_major=False)
    result = tf.matmul(final_state[1], weights['out']) + biases['out']
    return result

def RNN_V2(X, weights, biases):
    lstm_cell = tf.contrib.rnn.BasicLSTMCell(n_hidden, forget_bias=1.0)
    init_state = lstm_cell.zero_state(batch_size, dtype=tf.float32)
    outputs, final_state = tf.nn.dynamic_rnn(lstm_cell, X, initial_state=init_state, time_major=False)
    result = tf.matmul(final_state[1], weights['out']) + biases['out']
    return result


pred = RNN_V2(x, weights, biases)
cost = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=pred, labels=y))
train_op = tf.train.AdamOptimizer(lr).minimize(cost)

correct_pred = tf.equal(tf.argmax(pred, 1), tf.argmax(y, 1))
accuarcy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())
    step = 0
    while(step * batch_size < training_iters):
        batch_xs, batch_ys = mnist.train.next_batch(batch_size)
        batch_xs = batch_xs.reshape([batch_size, n_step, n_input])
        sess.run([train_op], feed_dict={x:batch_xs, y:batch_ys})
        if step % 20 == 0:
            print(sess.run(accuarcy, feed_dict={x:batch_xs, y:batch_ys}))
        step += 1

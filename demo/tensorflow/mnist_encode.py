#!/usr/bin/python
# -*- encoding:utf-8 -*-
from tensorflow.examples.tutorials.mnist import input_data
import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
print(tf.__version__)

mnist = input_data.read_data_sets('/home/zhongjuliu/projects/machineLearning/dataset', one_hot=True)

trX, trY, teX, teY = mnist.train.images, mnist.train.labels, mnist.test.images, mnist.test.labels

learn_rate = 0.01
train_epochs = 20
batch_size = 256
display_step = 1

n_input = 28 * 28

example_to_show = 10

x = tf.placeholder(tf.float32, [None, n_input])

n_hidden_1 = 256
n_hidden_2 = 128

weights = {
    'encode_h1': tf.Variable(tf.random_normal([n_input, n_hidden_1])),
    'encode_h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
    'decode_h1': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_1])),
    'decode_h2': tf.Variable(tf.random_normal([n_hidden_1, n_input]))
}

biases = {
    'encode_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'encode_b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'decode_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'decode_b2': tf.Variable(tf.random_normal([n_input]))
}

def encode(x):
    layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['encode_h1']), biases['encode_b1']))
    layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['encode_h2']), biases['encode_b2']))

    return layer_2

def decode(x):
    layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['decode_h1']), biases['decode_b1']))
    layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['decode_h2']), biases['decode_b2']))

    return layer_2

encode_op = encode(x)
decode_op = decode(encode_op)

y_pred = decode_op
y_true = x

cost = tf.reduce_mean(tf.pow(y_true - y_pred, 2))
optmizer = tf.train.RMSPropOptimizer(learning_rate=learn_rate).minimize(cost)

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())
    total_batch = int(mnist.train.num_examples/batch_size)
    for epoch in range(train_epochs):
        for i in range(total_batch):
            batch_xs, batch_ys = mnist.train.next_batch(batch_size)
            _, c = sess.run([optmizer, cost], feed_dict={x: batch_xs})

        if epoch % display_step == 0:
            print("Epoch:{0:04d},cost:{1:.9f}").format(epoch+1, c)

    print('optmize finish')

    encode_decode = sess.run(y_pred, feed_dict={x:mnist.test.images[:example_to_show]})
    f,a = plt.subplots(2,10,figsize=(10,2))
    for i in range(example_to_show):
        a[0][i] = plt.imshow(np.reshape(mnist.test.images[i], (28, 28)))
        a[1][i] = plt.imshow(np.reshape(encode_decode[i], (28,28)))
    f.show()
    plt.draw()
    # plt.waitforbuttonpress()


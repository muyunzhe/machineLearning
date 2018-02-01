# -*-coding:utf-8-*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np
import pandas as pd
from six.moves import urllib
from six.moves import xrange  # pylint: disable=redefined-builtin
import tensorflow as tf
from tempfile import gettempdir

import math
import os
import random
import sys

flags = tf.app.flags

flags.DEFINE_string("save_path", None, "Direactory to write the model and item vectors.")

flags.DEFINE_string("load_file", None, "Training text file.")

flags.DEFINE_integer("vocabulary_size", None, "Item size")
flags.DEFINE_string("suffix", "", "suffix of NA and UNK")
flags.DEFINE_string("similarity_file", None, "A File to store similarity_file")
flags.DEFINE_string("embed_file", None, "A File to store embed matrix")
flags.DEFINE_string("hql_file", None, "A File to store hql code")
flags.DEFINE_string("hql_table_name", None, "hql table name")
flags.DEFINE_integer("embedding_size", 64, "The embedding dimension size.")
flags.DEFINE_integer("epochs_to_train", 15, "Number of epochs to train. Each epoch processes the training data once")
flags.DEFINE_float("learning_rate", 0.2, "Initial learning rate.")
flags.DEFINE_integer("batch_size", 128, "Number of training examples process per step")
flags.DEFINE_integer("num_steps", 10001, "The number of training steps")
flags.DEFINE_integer("skip_window", 1, "The number of words to predict to the left and right of the target word")
flags.DEFINE_integer("min_count", 5, "The minimum number of word occurrences for it to be included in the vocabulary")
flags.DEFINE_integer("num_sampled", 64, "Number of negative examples to sample")
flags.DEFINE_integer("num_skips", 2, "How many times to reuse an input to generate a label")
flags.DEFINE_integer("valid_size", 16, "Random set of words to evaluate similarity on")
flags.DEFINE_integer("valid_window", 100, "Only pick dev samples in the head of the distribution")



FLAGS = flags.FLAGS


class Options(object):
    def __init__(self):
        # Model Options.

        # Embedding dimension.
        self.emb_dim = FLAGS.embedding_size

        # Training options.
        self.load_file = FLAGS.load_file

        # Number of negative samples per example.
        self.num_sampled = FLAGS.num_sampled

        # The initial learning rate.
        self.learning_rate = FLAGS.learning_rate

        # Number of epochs to train. After these many epochs, the
        # learning rate decays liearnly to zero and the training stops.
        self.epochs_to_train = FLAGS.epochs_to_train

        # Concurrent training steps.
        self.num_steps = FLAGS.num_steps

        # Number of examples for one training step
        self.batch_size = FLAGS.batch_size

        # The number of words to predict to the left and right of the target word.
        self.skip_window = FLAGS.skip_window

        # How many times to reuse an input to generate a label
        self.num_skips = FLAGS.num_skips

        # The minimum number of word occurrences for it to be included in the vocabulary.
        self.min_count = FLAGS.min_count

        # Random set of words to evaluate similarity on.
        self.valid_size = FLAGS.valid_size

        # Only pick dev samples in the head of the distribution.
        self.valid_window = FLAGS.valid_window

        # Subsampling threshold for word occurrence.
        self.num_sampled = FLAGS.num_sampled

        # Where to write out summaries.
        self.save_path = FLAGS.save_path
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)

        # Eval options.
        # The text file to store similarity file.
        self.similarity_file = FLAGS.similarity_file

        # The file to store embed matrix
        self.embed_file = FLAGS.embed_file

        # Vocabulary Size.
        self.vocabulary_size = FLAGS.vocabulary_size

        # Valid examples.
        self.valid_examples = np.random.choice(FLAGS.valid_window, FLAGS.valid_size, replace=False)

        # Hql File.
        self.hql_file = FLAGS.hql_file

        # Hql Table Name.
        self.hql_table_name = FLAGS.hql_table_name
        
        # suffix.
        self.suffix = FLAGS.suffix



class Item2Vec(object):
    """Word2Vec Model(Skipgram)"""

    def __init__(self, options):
        self._options = options
        self._word2id = {}
        self._id2word = []
        # self.build_graph()
        self.data_index = 0

    def read_data(self):
        item_lst = []
        f = open(self._options.load_file, "r")

        for line in f.readlines():
            line = line.strip()  ##去除空格
            if not len(line) or line.startswith('#'):  ##去除注释行
                continue
            item_name = line.split("\t")[1]  ##把一个user_id的酒店获取
            item_name = item_name.translate(None, '[]').split(',')  ##把一个user_id的酒店切分
            item_name.append('{}NA'.format(self._options.suffix))
            for item in item_name:
                item_lst.append(item)  ##把每一个酒店存在list中

        f.close()

        print('Item Numbers: %s' % len(set(item_lst)))
        self._item_lst = item_lst
        if self._options.vocabulary_size > len(set(item_lst)):
            print("--vocabulary_size must be less than or equal {}".format(len(set(item_lst))))
            sys.exit(1)
        
        

    def build_dataset(self):
        words = self._item_lst
        n_words = self._options.vocabulary_size

        count = [['{}UNK'.format(self._options.suffix), -1]]
        count.extend(collections.Counter(words).most_common(n_words - 1))
        dictionary = dict()
        for word, _ in count:
            dictionary[word] = len(dictionary)
        data = list()
        unk_count = 0
        for word in words:
            index = dictionary.get(word, 0)
            if index == 0:  # dictionary['UNK']
                unk_count += 1
            data.append(index)
        count[0][1] = unk_count
        reversed_dictionary = dict(zip(dictionary.values(), dictionary.keys()))
        self._data = data
        self._count = count
        self._dictionary = dictionary
        self._reversed_dictionary = reversed_dictionary

    def generate_batch(self):
        assert self._options.batch_size % self._options.batch_size == 0
        assert self._options.num_skips <= 2 * self._options.skip_window
        batch = np.ndarray(shape=(self._options.batch_size), dtype=np.int32)
        labels = np.ndarray(shape=(self._options.batch_size, 1), dtype=np.int32)
        span = 2 * self._options.skip_window + 1
        buffer = collections.deque(maxlen=span)
        if self.data_index + span > len(self._data):
            self.data_index = 0
        buffer.extend(self._data[self.data_index:self.data_index + span])
        self.data_index += span
        for i in range(self._options.batch_size // self._options.num_skips):
            context_words = [w for w in range(span) if w != self._options.skip_window]
            words_to_use = random.sample(context_words, self._options.num_skips)
            for j, context_word in enumerate(words_to_use):
                batch[i * self._options.num_skips + j] = buffer[self._options.skip_window]
                labels[i * self._options.num_skips + j, 0] = buffer[context_word]
            if self.data_index == len(self._data):
                buffer[:] = self._data[:span]
                self.data_index = span
            else:
                buffer.append(self._data[self.data_index])
                self.data_index += 1

        self.data_index = (self.data_index + len(self._data) - span) % len(self._data)
        return batch, labels

    def build_graph(self):

        # Input data.
        self.train_inputs = tf.placeholder(tf.int32, shape=[self._options.batch_size])
        self.train_labels = tf.placeholder(tf.int32, shape=[self._options.batch_size, 1])
        self.valid_dataset = tf.constant(self._options.valid_examples, dtype=tf.int32)

        with tf.device('/CPU:0'):
            embeddings = tf.Variable(
                tf.random_uniform([self._options.vocabulary_size, self._options.emb_dim], -1.0, 1.0))
            embed = tf.nn.embedding_lookup(embeddings, self.train_inputs)

            # Construct the variables for the NCE loss
            nce_weights = tf.Variable(
                tf.truncated_normal([self._options.vocabulary_size, self._options.emb_dim],
                                    stddev=1.0 / math.sqrt(self._options.emb_dim)))
            nce_biases = tf.Variable(tf.zeros([self._options.vocabulary_size]))

        # Compute the average NCE loss for the batch.
        self._loss = tf.reduce_mean(
            tf.nn.nce_loss(weights=nce_weights,
                           biases=nce_biases,
                           labels=self.train_labels,
                           inputs=embed,
                           num_sampled=self._options.num_sampled,
                           num_classes=self._options.vocabulary_size))

        # Construct the SGD optimizer using a learning rate of 1.0.
        self._optimizer = tf.train.GradientDescentOptimizer(1.0).minimize(self._loss)

        # Compute the cosine similarity between minibatch examples and all embeddings.
        norm = tf.sqrt(tf.reduce_sum(tf.square(embeddings), 1, keep_dims=True))
        self._normalized_embeddings = embeddings / norm
        valid_embeddings = tf.nn.embedding_lookup(
            self._normalized_embeddings, self.valid_dataset)
        self._similarity = tf.matmul(
            valid_embeddings, self._normalized_embeddings, transpose_b=True)

        # Add variable initializer.
        self._init = tf.global_variables_initializer()

    def train(self):
        with tf.Session() as session:
            # We must initialize all variables before we use them.
            session.run(self._init)
            print('Initialized')

            average_loss = 0

            f = open(self._options.save_path +"/" +self._options.similarity_file, 'w')  # write log to file

            for step in xrange(self._options.num_steps):
                # print(step)
                batch_inputs, batch_labels = self.generate_batch()
                feed_dict = {self.train_inputs: batch_inputs, self.train_labels: batch_labels}

                _, loss_val = session.run([self._optimizer, self._loss], feed_dict=feed_dict)
                average_loss += loss_val

                if step % 20 == 0:
                    if step > 0:
                        average_loss /= 20
                    # The average loss is an estimate of the loss over the last 2000 batches.
                    print('Average loss at step ', step, ': ', round(average_loss, 4))
                    average_loss = 0

                # Note that this is expensive (~20% slowdown if computed every 500 steps)
                if step % 50 == 0:
                    sim = self._similarity.eval()
                    for i in xrange(self._options.valid_size):
                        valid_word = self._reversed_dictionary[self._options.valid_examples[i]]
                        top_k = 8  # number of nearest neighbors
                        nearest = (-sim[i, :]).argsort()[1:top_k + 1]
                        log_str = 'Step:%d , Nearest to %s:' % (step,valid_word)
                        for k in xrange(top_k):
                            close_word = self._reversed_dictionary[nearest[k]]
                            log_str = '%s %s,' % (log_str, close_word)
                        f.write(log_str + "\n")
                        # print(log_str)
            f.close()
            self._final_embeddings = self._normalized_embeddings.eval()

    def embed_export(self):
        final_embeddings_df = pd.DataFrame(self._final_embeddings)
        print("Embedding Shape:{},{}".format(final_embeddings_df.shape[0],final_embeddings_df.shape[1]))

        item_list = self._reversed_dictionary.values()
        item_list = [item.replace("\"", "") for item in item_list]

        final_embeddings_df.insert(loc=0, column='item', value=item_list)
        final_embeddings_df.to_csv(self._options.save_path +"/" +self._options.embed_file, header=False, index=False)

    def hql_export(self):
        table_name = self._options.hql_table_name
        emd_size = self._options.emb_dim

        str1 = "drop table if exists {};".format(table_name)
        str2 = "create table if not exists {}(".format(table_name)
        col_lst = ["col{} double".format(i + 1) for i in range(emd_size)]

        hql = open(self._options.save_path +"/"+self._options.hql_file, 'w')
        hql.write(str1 + "\n")
        hql.write(str2 + "\n")
        hql.write("item_name string" + ",\n")
        for i, col in enumerate(col_lst):
            if i != len(col_lst) - 1:
                hql.write(col + ",\n")
            else:
                hql.write(col)
        hql.write(") \n")
        hql.write("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';")
        hql.close()
        print("Finish Export HQL Code!")
        

def main():
    """Train a model."""
    if not FLAGS.load_file or not FLAGS.save_path:
        print("--load_file or --save_path must be specified.")
        sys.exit(1)

    opts = Options()
    model = Item2Vec(opts)
    model.read_data()
    model.build_dataset()
    model.build_graph()
    model.train()
    model.embed_export()
    model.hql_export()


if __name__ == "__main__":
    main()


import numpy as np

class Perceptron():
  def __init__(self, eta=0.01, n_iter=10):
        self.eta = eta
        self.n_iter = n_iter

  def fit(self, X, y):

#!/usr/bin/python
# -*- encoding:utf-8 -*-

import numpy as np

class Layer:
    def __init__(self):
        pass
    def forward(self, input):
        return input
    def backward(self, input, grad_output):
        pass


# 定义Relu层
class ReLU(Layer):
    def __init__(self):
        pass
    def forward(self,input):
        return np.maximum(0,input) # relu函数为max(0,x)
    def backward(self,input,grad_output):
        relu_grad = input>0        #relu函数导数为1 if x>0 else 0
        return grad_output*relu_grad


class Sigmoid(Layer):
    def __init__(self):
        pass

    def _sigmoid(self, x):
        return 1.0 / (1 + np.exp(-x))

    def forward(self, input):
        return self._sigmoid(input)

    def backward(self, input, grad_output):
        sigmoid_grad = self._sigmoid(input) * (1 - self._sigmoid(input))
        return grad_output * sigmoid_grad


class Tanh(Layer):
    def __init__(self):
        pass
    def _tanh(self,x):
        return np.tanh(x)
    def forward(self,input):
        return self._tanh(input)
    def backward(self, input, grad_output):
        grad_tanh = 1-(self._tanh(input))**2
        return grad_output*grad_tanh
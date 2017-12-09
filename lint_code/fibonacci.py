#-*- encoding: utf-8 -*-

from functools import wraps
from collections import Iterator
import time
import numpy


def simple_fib(n):
    assert n >= 0
    if n < 2:
        return n
    return simple_fib(n - 1) + simple_fib(n - 2)


def cache(func):
    _cache = {}

    @wraps(func)
    def wrapper(n):
        if n in _cache:
            return _cache[n]
        r = func(n)
        _cache[n] = r
        return r


@cache
def cache_fib(n):
    assert n > 0
    if n < 2:
        return n
    return simple_fib(n - 1) + simple_fib(n - 2)


def loop_fib(n):
    """
    时间复杂度为O(n)，空间复杂度为O(1)
    :param n:
    :return:
    """
    if n < 0:
        raise ValueError("n must be positive or zero")
    a, b = 0, 1
    for _ in range(n - 1):
        a, b = b, a + b
    return b


def matrix_fib(n):
    assert n > 0
    if n < 2:
        return n
    return numpy.matrix([[1, 1], [1, 0]] ** (n - 1) * numpy.matrix([1], [0]))[0, 0]


class Fib:
    def __init__(self):
        self.a = 0
        self.b = 1

    def __iter__(self):
        return self

    def __next__(self):
        value = self.b
        self.a, self.b = self.b, self.a + self.b
        return value


if __name__ == "__main__":
    
    epoch = 30
    
    start_time = time.time()
    for i in range(epoch):
        result=simple_fib(i)
    end_time = time.time()
    print('simple_fib cost time = {time}').format(time=end_time - start_time)

    start_time = time.time()
    for i in range(epoch):
        result = simple_fib(i)
    end_time = time.time()
    print('cache_fib cost time = {time}').format(time=end_time - start_time)

    start_time = time.time()
    for i in range(epoch):
        result = simple_fib(i)
    end_time = time.time()
    print('loop_fib cost time = {time}').format(time=end_time - start_time)

    start_time = time.time()
    for i in range(epoch):
        result = simple_fib(i)
    end_time = time.time()
    print('matrix_fib cost time = {time}').format(time=end_time - start_time)

    start_time = time.time()
    fib = Fib()
    for i in range(epoch):
        result = fib.__next__()
    end_time = time.time()
    print('iter_fib cost time = {time}').format(time=end_time - start_time)

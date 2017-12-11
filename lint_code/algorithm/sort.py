#!/usr/bin/python
# -*- encoding:utf-8 -*-
from collections import Counter
from random import randint


def find_most_char(s):
    l = [c for c in s]
    print Counter(l)


if __name__ == "__main__":
    # s = "&(&$#(JFLSDJFOIWEfdjsklfewosd2378907345asdsdwsdqs"
    # find_most_char(s)
    keys = 'asdf'
    d = {x:randint(90,100) for x in keys}

    print d.items()

    print sorted(d.items(), key=lambda x: x[1])

    print d.items()

#!/bin/sh

export LD_LIBRARY_PATH=newkeras/newkeras/external_lib/:$LD_LIBRARY_PATH

newkeras/newkeras/external_lib/ld-linux-x86-64.so.2 newkeras/newkeras/bin/python fusion_net_predict_map.py
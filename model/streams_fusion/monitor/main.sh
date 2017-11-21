#!/bin/bash

function printlog(){
    echo `date +"%Y-%m-%d %H:%M:%S"` $1
}

bash /home/q/bizdata/data/order_risk/streams_fusion/monitor/monitor_user.sh

if [ $? -ne 0 ]; then
    printlog "monitor user失败"
    exit 1
fi
printlog "monitor user完成"

bash /home/q/bizdata/data/order_risk/streams_fusion/monitor/monitor_order.sh

if [ $? -ne 0 ]; then
    printlog "monitor order失败"
    exit 1
fi
printlog "monitor order完成"
#!/bin/bash

if [ $# -eq 0 ]; then
    trace_date=`date -d "99 days ago" +"%Y-%m-%d"`
else
    trace_date="$1"
fi

echo "Trace date: ${trace_date}"

sudo /bin/bash /home/q/bizdata/data/order_risk/streams_fusion/predict/main.sh ${trace_date}
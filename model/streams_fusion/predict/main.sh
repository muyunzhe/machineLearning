#!/bin/bash

# Notes:
# 上一版的例行在http://gitlab.corp.qunar.com/bizdata/data/blob/init/order_risk/users_inloan/predict/main.sh
# 生成用户候选集合(截止到本次例行去哪儿端拿去花激活成功的用户才可入选)共用上一版例行的成果就OK
# 特征分为两部分 
# but，暂时先各例行各的，条件允许情况下，快速上线

if [ $# -eq 0 ]; then
    base_date=`date -d "yesterday" +"%Y-%m-%d"`
else
    base_date="$1"
fi

today=`date -d "${base_date} 1 days" +"%Y-%m-%d"`

function printlog(){
    echo `date +"%Y-%m-%d %H:%M:%S"` "$1"
}

printlog "-----基准时间:${base_date}-开始本次任务-----"

# Step 0: 新建预测结果表，仅本脚本首次运行触发

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
create table if not exists orderrisk.users_inloan_fusionnet_predict_result (
user_id        string        comment '用户ID',
prob           double        comment '风险概率'
)
comment '用户贷中逾期风险结果表(powered by Fusion Net)'
partitioned by (
dt        string        comment '基准日期'
)
row format delimited
    fields terminated by '\t'
    lines  terminated by '\n'
stored as 
inputformat
    'org.apache.hadoop.mapred.TextInputFormat'
outputformat
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
"

if [ $? -ne 0 ]; then
    printlog "Step 0: 新建预测结果表失败"
    exit 1
fi
printlog "Step 0: 新建预测结果表成功"

# Step 1: 生成预测候选用户集，截止到本次例行去哪儿端拿去花激活成功的用户才可入选
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
drop table if exists orderrisk.users_inloan_fusionnet_predict_user_set;
create table orderrisk.users_inloan_fusionnet_predict_user_set as

select user_id, null as begin_date, '${today}' as end_date
from dw_cube.cube_credit_activate_contract
-- 去哪儿端拿去花/商旅贷产品，激活成功的用户
WHERE org_channel = 'QUNAR' and product_no in ('IOUS', 'BUSI') and activate_status = 2 and to_date(activate_finish_time) <= '${base_date}'
group by user_id;
"

if [ $? -ne 0 ]; then
    printlog "Step 1: 生成预测候选用户失败"
    exit 1
fi
printlog "Step 1: 生成预测候选用户成功"

# Step 2: 生成预测候选用户snap特征
/home/q/anaconda2/envs/newkeras/bin/python /home/q/bizdata/data/shell/tools/python/feature_generation/the_moment_dimension/generate_event_feature.py /home/q/bizdata/data/order_risk/streams_fusion/predict/users_inloan_features.json

if [ $? -ne 0 ]; then
    printlog "Step 2: 生成预测候选用户snap特征失败"
    exit 1
fi
printlog "Step 2: 生成预测候选用户snap特征成功"

# Step 3: 生成预测候选用户ts特征
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
drop table if exists orderrisk.users_inloan_fusionnet_predict_user_set_month;

create table orderrisk.users_inloan_fusionnet_predict_user_set_month as

select user_id, begin_month, end_month
from 
(
    select user_id,
    concat_ws('-', cast(year(end_date) - 4 as string), lpad(month(end_date), 2, 0)) as begin_month,
    substr(end_date, 1, 7) as end_month
    from orderrisk.users_inloan_fusionnet_predict_user_set
) a
group by user_id, begin_month, end_month;
"

bash /home/q/bizdata/data/shell/tools/python/feature_generation/time_series/run_ts_features.sh orderrisk.users_inloan_fusionnet_predict_user_set_month orderrisk.users_inloan_fusionnet_predict_ts_features

if [ $? -ne 0 ]; then
    printlog "Step 3: 生成预测候选用户ts特征失败"
    exit 1
fi
printlog "Step 3: 生成预测候选用户ts特征成功"

# Step 4: 组合快照型和时间序列型特征
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
drop table if exists orderrisk.users_inloan_fusionnet_predict_features;

create table orderrisk.users_inloan_fusionnet_predict_features
row format delimited
    fields terminated by '\t'
    lines  terminated by '\n' as

select t1.*, t2.features_json
from orderrisk.users_inloan_fusionnet_predict_snap_features t1
join orderrisk.users_inloan_fusionnet_predict_ts_features t2
on t1.user_id = t2.user_id and substr(t1.end_date, 1, 7) = t2.end_month;
"

if [ $? -ne 0 ]; then
    printlog "Step 4: 组合快照型和时间序列型特征失败"
    exit 1
fi
printlog "Step 4: 组合快照型和时间序列型特征成功"

# Step 5: hadoop streaming进行预测

sudo -ubizdata hadoop fs -rmr hdfs://qunarcluster/user/bizdata/hive/warehouse/orderrisk.db/users_inloan_fusionnet_predict_result/dt=${base_date}

sudo -ubizdata hadoop jar /home/q/hadoop/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -D mapred.job.name="FusionNet_Prediction_${base_date}" \
    -D mapreduce.job.reduces=0 \
    -files /home/q/bizdata/data/order_risk/streams_fusion/predict/fusion_net_predict_map.sh,/home/q/bizdata/data/order_risk/streams_fusion/predict/fusion_net_predict_map.py,/home/q/bizdata/data/order_risk/streams_fusion/base/unzip_ts_features.py,/home/q/bizdata/temp/fusion_net_model/fusion_net_final.h5,/home/q/bizdata/temp/fusion_net_model/fusion_net_final.json,/home/q/bizdata/temp/fusion_net_model/fusion_snap_mean_final.npy,/home/q/bizdata/temp/fusion_net_model/fusion_snap_std_final.npy,/home/q/bizdata/temp/fusion_net_model/fusion_ts_mean_final.npy,/home/q/bizdata/temp/fusion_net_model/fusion_ts_std_final.npy,/home/q/bizdata/data/order_risk/streams_fusion/predict/feature_map.json \
    -archives /home/q/bizdata/temp/fusion_net_model/newkeras.tar.gz#newkeras \
    -input hdfs://qunarcluster/user/bizdata/hive/warehouse/orderrisk.db/users_inloan_fusionnet_predict_features \
    -output hdfs://qunarcluster/user/bizdata/hive/warehouse/orderrisk.db/users_inloan_fusionnet_predict_result/dt=${base_date} \
    -mapper 'sh fusion_net_predict_map.sh'

if [ $? -ne 0 ]; then
    printlog "Step 5: hadoop streaming预测失败"
    exit 1
fi
printlog "Step 5: hadoop streaming预测成功"

# Step 6: 创建hive分区
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
alter table orderrisk.users_inloan_fusionnet_predict_result add if not exists partition(dt='${base_date}');
"

if [ $? -ne 0 ]; then
    printlog "Step 6: 创建hive分区失败"
    exit 1
fi
printlog "Step 6: 创建hive分区成功"

printlog "-----基准时间:${base_date}-本次例行圆满结束-----"

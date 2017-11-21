#!/bin/bash

if [ $# -eq 0 ]; then
    perf_date=`date -d "100 days ago" +"%Y-%m-%d"`
else
    perf_date="$1"
fi

distribute_date=`date -d "yesterday" +"%Y-%m-%d"`

echo "performance date: ${perf_date}"
echo "distribute date: ${distribute_date}"

function printlog(){
  echo `date +"%Y-%m-%d %H:%M:%S"` "$1"
}

printlog "-----基准时间:${distribute_date}-开始本次任务-----"

# Step 0: 生成用户最新分数分布

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists monitor.users_inloan_fusionnet_prob_level_distribute;

CREATE TABLE monitor.users_inloan_fusionnet_prob_level_distribute as

SELECT a.user_id, 
if(b.user_id is not null, 1, 0) as has_newest_order,
case 
when prob >= 0 and prob <= 5   then '(00,05]' 
when prob > 5  and prob <= 10  then '(05,10]'
when prob > 10 and prob <= 15  then '(10,15]' 
when prob > 15 and prob <= 20  then '(15,20]'
when prob > 20 and prob <= 25  then '(20,25]' 
when prob > 25 and prob <= 30  then '(25,30]'
when prob > 30 and prob <= 35  then '(30,35]' 
when prob > 35 and prob <= 40  then '(35,40]'
when prob > 40 and prob <= 45  then '(40,45]' 
when prob > 45 and prob <= 50  then '(45,50]'
when prob > 50 and prob <= 55  then '(50,55]' 
when prob > 55 and prob <= 60  then '(55,60]'
when prob > 60 and prob <= 65  then '(60,65]' 
when prob > 65 and prob <= 70  then '(65,70]'
when prob > 70 and prob <= 75  then '(70,75]' 
when prob > 75 and prob <= 80  then '(75,80]'
when prob > 80 and prob <= 85  then '(80,85]' 
when prob > 85 and prob <= 90  then '(85,90]'
when prob > 90 and prob <= 95  then '(90,95]' 
when prob > 95 and prob <= 100 then '(95,100]'
end as prob_level
FROM (
SELECT user_id, floor(round(prob, 2) * 100) as prob
FROM orderrisk.users_inloan_fusionnet_predict_result
WHERE dt = '${distribute_date}'
) a

LEFT OUTER JOIN

(
SELECT user_id 
FROM dw_cube.cube_order_repayment
WHERE org_channel = 'QUNAR' and product_no = 'IOUS' and
      to_date(order_time) = '${distribute_date}'
GROUP BY user_id
) b
ON a.user_id = b.user_id;
"

if [ $? -ne 0 ]; then
    printlog "Step 0: 生成用户最新分数分布失败"
    exit 1
fi
printlog "Step 0: 生成用户最新分数分布成功"

# Step 1: 生成用户评测分数分布以及逾期情况
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists monitor.users_inloan_fusionnet_prob_level_performance;

CREATE TABLE monitor.users_inloan_fusionnet_prob_level_performance as

SELECT t1.user_id, t1.prob_level, 
t2.overdue_label, t2.overdue_30days_label, t2.overdue_60days_label,
t2.overdue_90days_label, t2.has_performance,
if(t3.user_id is not null, 1, 0) as has_perf_order
FROM (
SELECT user_id,
case 
when prob >= 0 and prob <= 5   then '(00,05]' 
when prob > 5  and prob <= 10  then '(05,10]'
when prob > 10 and prob <= 15  then '(10,15]' 
when prob > 15 and prob <= 20  then '(15,20]'
when prob > 20 and prob <= 25  then '(20,25]' 
when prob > 25 and prob <= 30  then '(25,30]'
when prob > 30 and prob <= 35  then '(30,35]' 
when prob > 35 and prob <= 40  then '(35,40]'
when prob > 40 and prob <= 45  then '(40,45]' 
when prob > 45 and prob <= 50  then '(45,50]'
when prob > 50 and prob <= 55  then '(50,55]' 
when prob > 55 and prob <= 60  then '(55,60]'
when prob > 60 and prob <= 65  then '(60,65]' 
when prob > 65 and prob <= 70  then '(65,70]'
when prob > 70 and prob <= 75  then '(70,75]' 
when prob > 75 and prob <= 80  then '(75,80]'
when prob > 80 and prob <= 85  then '(80,85]' 
when prob > 85 and prob <= 90  then '(85,90]'
when prob > 90 and prob <= 95  then '(90,95]' 
when prob > 95 and prob <= 100 then '(95,100]'
end as prob_level
FROM (
SELECT user_id, floor(round(prob, 2) * 100) as prob
FROM orderrisk.users_inloan_fusionnet_predict_result
WHERE dt = '${perf_date}'
) a
) t1

LEFT OUTER JOIN

(
SELECT user_id, 
max(if(od_days_cnt >= 1,  1, 0)) as overdue_label,
max(if(od_days_cnt >= 30, 1, 0)) as overdue_30days_label,
max(if(od_days_cnt >= 60, 1, 0)) as overdue_60days_label,
max(if(od_days_cnt >= 90, 1, 0)) as overdue_90days_label,
max(if(datediff('${distribute_date}', order_time) > 30, 1, 0)) as has_performance
FROM dw_cube.cube_term_repayment
WHERE product_no = 'IOUS' and org_channel = 'QUNAR' and to_date(order_time) > '${perf_date}'
GROUP BY user_id
) t2
ON t1.user_id = t2.user_id

LEFT OUTER JOIN

(
SELECT user_id
FROM dw_cube.cube_order_repayment
WHERE org_channel = 'QUNAR' and product_no = 'IOUS' and
      to_date(order_time) = '${perf_date}'
GROUP BY user_id
) t3
ON t1.user_id = t3.user_id;
"

if [ $? -ne 0 ]; then
    printlog "Step 1: 生成用户评测分数分布以及逾期情况失败"
    exit 1
fi
printlog "Step 1: 生成用户评测分数分布以及逾期情况成功"

# Step 2: 生成模型效果监控表
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
CREATE TABLE if not exists monitor.users_inloan_fusionnet_performance_monitor (
prob_level                   string comment '分数段(越高越坏)',

newest_total_users           int    comment '最新用户数',
perf_total_users             int    comment '表现期开始用户数',
not_overdue_users            int    comment '无逾期用户数',
overdue_users                int    comment '有逾期用户数',
overdue_30_users             int    comment '有逾期30天+用户数',
overdue_60_users             int    comment '有逾期60天+用户数',
overdue_90_users             int    comment '有逾期90天+用户数',

has_order_newest_total_users           int    comment '最新有当日拿去花订单的用户数',
has_order_perf_total_users             int    comment '表现期开始有当日拿去花订单的用户数',
has_order_not_overdue_users            int    comment '表现期开始有当日拿去花订单且后面无逾期用户数',
has_order_overdue_users                int    comment '表现期开始有当日拿去花订单且后面有逾期用户数',
has_order_overdue_30_users             int    comment '表现期开始有当日拿去花订单且后面有逾期30天+用户数',
has_order_overdue_60_users             int    comment '表现期开始有当日拿去花订单且后面有逾期60天+用户数',
has_order_overdue_90_users             int    comment '表现期开始有当日拿去花订单且后面有逾期90天+用户数'
)
PARTITIONED BY (dt string comment '基准日期')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE monitor.users_inloan_fusionnet_performance_monitor partition (dt = '${distribute_date}')

SELECT a.prob_level, a.newest_total_users, b.perf_total_users,
b.not_overdue_users, b.overdue_users,
b.overdue_30_users, b.overdue_60_users, b.overdue_90_users,

a.has_order_newest_total_users, b.has_order_perf_total_users,
b.has_order_not_overdue_users, b.has_order_overdue_users,
b.has_order_overdue_30_users, b.has_order_overdue_60_users,
b.has_order_overdue_90_users
FROM (
SELECT prob_level, 
       count(distinct user_id) as newest_total_users,
       count(distinct if(has_newest_order = 1, user_id, null)) as has_order_newest_total_users
FROM monitor.users_inloan_fusionnet_prob_level_distribute
GROUP BY prob_level
) a

JOIN

(
SELECT prob_level, 
count(distinct user_id) as perf_total_users,
count(distinct if(has_performance = 1 and overdue_label = 0, user_id, null)) as not_overdue_users,
count(distinct if(overdue_label = 1, user_id, null)) as overdue_users,
count(distinct if(overdue_30days_label = 1, user_id, null)) as overdue_30_users,
count(distinct if(overdue_60days_label = 1, user_id, null)) as overdue_60_users,
count(distinct if(overdue_90days_label = 1, user_id, null)) as overdue_90_users,

count(distinct if(has_perf_order = 1, user_id, null)) as has_order_perf_total_users,
count(distinct if(has_perf_order = 1 and has_performance = 1 and overdue_label = 0, user_id, null)) as has_order_not_overdue_users,
count(distinct if(has_perf_order = 1 and overdue_label = 1, user_id, null)) as has_order_overdue_users,
count(distinct if(has_perf_order = 1 and overdue_30days_label = 1, user_id, null)) as has_order_overdue_30_users,
count(distinct if(has_perf_order = 1 and overdue_60days_label = 1, user_id, null)) as has_order_overdue_60_users,
count(distinct if(has_perf_order = 1 and overdue_90days_label = 1, user_id, null)) as has_order_overdue_90_users
FROM monitor.users_inloan_fusionnet_prob_level_performance
GROUP BY prob_level
) b
ON a.prob_level = b.prob_level;
"

if [ $? -ne 0 ]; then
    printlog "Step 2: 生成模型监控表失败"
    exit 1
fi
printlog "Step 2: 生成模型监控表成功"

# Step 3: 将全量结果导入mysql数据库
monitor_path='/home/q/bizdata/temp/monitor'
if [ ! -d ${monitor_path} ]
then
    mkdir ${monitor_path}
fi

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
SELECT prob_level, newest_total_users, perf_total_users, not_overdue_users,
overdue_users, overdue_30_users, overdue_60_users, overdue_90_users,
dt
FROM monitor.users_inloan_fusionnet_performance_monitor WHERE dt = '${distribute_date}' and prob_level is not null;
" > ${monitor_path}/users_inloan_fusionnet_performance_result

sudo -ubizdata mysql -h192.168.52.62 -uhive -p'Aqas~1234~!@' -P3306 --default-character-set=utf8 -A report --local-infile=1 -e "
set names utf8;

CREATE TABLE if not exists users_inloan_fusionnet_performance_result_total (
prob_level         varchar(50) not null,
newest_total_users int default 0,
perf_total_users   int default 0,
not_overdue_users  int default 0,
overdue_users      int default 0,
overdue_30_users   int default 0,
overdue_60_users   int default 0,
overdue_90_users   int default 0,
dt                 varchar(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
delete from users_inloan_fusionnet_performance_result_total where dt = '${distribute_date}';
LOAD DATA LOCAL INFILE '${monitor_path}/users_inloan_fusionnet_performance_result' INTO TABLE users_inloan_fusionnet_performance_result_total FIELDS TERMINATED BY '\t';
"

rm -f ${monitor_path}/users_inloan_fusionnet_performance_result

if [ $? -ne 0 ]; then
    printlog "将全量结果导入mysql数据库失败"
    exit 1
fi
printlog "将全量结果导入mysql数据库成功"

# Step 4: 将增量结果导入mysql数据库
monitor_path='/home/q/bizdata/temp/monitor'
if [ ! -d ${monitor_path} ]
then
    mkdir ${monitor_path}
fi

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
SELECT prob_level, has_order_newest_total_users, has_order_perf_total_users, has_order_not_overdue_users,
has_order_overdue_users, has_order_overdue_30_users, has_order_overdue_60_users, has_order_overdue_90_users,
dt
FROM monitor.users_inloan_fusionnet_performance_monitor WHERE dt = '${distribute_date}' and prob_level is not null;
" > ${monitor_path}/users_inloan_fusionnet_performance_result

sudo -ubizdata mysql -h192.168.52.62 -uhive -p'Aqas~1234~!@' -P3306 --default-character-set=utf8 -A report --local-infile=1 -e "
set names utf8;

CREATE TABLE if not exists users_inloan_fusionnet_performance_result_has_order (
prob_level         varchar(50) not null,
has_order_newest_total_users int default 0,
has_order_perf_total_users   int default 0,
has_order_not_overdue_users  int default 0,
has_order_overdue_users      int default 0,
has_order_overdue_30_users   int default 0,
has_order_overdue_60_users   int default 0,
has_order_overdue_90_users   int default 0,
dt                 varchar(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
delete from users_inloan_fusionnet_performance_result_has_order where dt = '${distribute_date}';
LOAD DATA LOCAL INFILE '${monitor_path}/users_inloan_fusionnet_performance_result' INTO TABLE users_inloan_fusionnet_performance_result_has_order FIELDS TERMINATED BY '\t';
"

rm -f ${monitor_path}/users_inloan_fusionnet_performance_result

if [ $? -ne 0 ]; then
    printlog "将增量结果导入mysql数据库失败"
    exit 1
fi
printlog "将增量结果导入mysql数据库成功"
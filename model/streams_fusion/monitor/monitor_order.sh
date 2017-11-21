#!/bin/bash

if [ $# -eq 0 ]; then
    order_date=`date -d "99 days ago" +"%Y-%m-%d"`
else
    order_date="$1"
fi

model_date=${order_date}

echo "order date: ${order_date}"
echo "model date: ${model_date}"

function printlog(){
    echo `date +"%Y-%m-%d %H:%M:%S"` "$1"
}

printlog "-----订单时间:${order_date}-开始本次任务-----"

# Step 0: 生成订单日前一日模型分数分布和订单日拿去花订单往后的逾期情况

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
drop table if exists monitor.users_inloan_fusionnet_order_detail;

create table monitor.users_inloan_fusionnet_order_detail as

select a.loan_provide_no,
case 
when b.prob >= 0 and b.prob <= 5   then '(00,05]' 
when b.prob > 5  and b.prob <= 10  then '(05,10]'
when b.prob > 10 and b.prob <= 15  then '(10,15]' 
when b.prob > 15 and b.prob <= 20  then '(15,20]'
when b.prob > 20 and b.prob <= 25  then '(20,25]' 
when b.prob > 25 and b.prob <= 30  then '(25,30]'
when b.prob > 30 and b.prob <= 35  then '(30,35]' 
when b.prob > 35 and b.prob <= 40  then '(35,40]'
when b.prob > 40 and b.prob <= 45  then '(40,45]' 
when b.prob > 45 and b.prob <= 50  then '(45,50]'
when b.prob > 50 and b.prob <= 55  then '(50,55]' 
when b.prob > 55 and b.prob <= 60  then '(55,60]'
when b.prob > 60 and b.prob <= 65  then '(60,65]' 
when b.prob > 65 and b.prob <= 70  then '(65,70]'
when b.prob > 70 and b.prob <= 75  then '(70,75]' 
when b.prob > 75 and b.prob <= 80  then '(75,80]'
when b.prob > 80 and b.prob <= 85  then '(80,85]' 
when b.prob > 85 and b.prob <= 90  then '(85,90]'
when b.prob > 90 and b.prob <= 95  then '(90,95]' 
when b.prob > 95 and b.prob <= 100 then '(95,100]'
else 'undefined' end as prob_level,
a.overdue_label,
a.overdue_30days_label,
a.overdue_60days_label,
a.overdue_90days_label,
a.has_performance

from
(
select user_id, loan_provide_no,
max(if(od_days_cnt >= 1,  1, 0)) as overdue_label,
max(if(od_days_cnt >= 30, 1, 0)) as overdue_30days_label,
max(if(od_days_cnt >= 60, 1, 0)) as overdue_60days_label,
max(if(od_days_cnt >= 90, 1, 0)) as overdue_90days_label,
max(if(current_date() > to_date(due_date), 1, 0)) as has_performance
from dw_cube.cube_term_repayment
WHERE product_no = 'IOUS' and org_channel = 'QUNAR' and to_date(order_time) = '${order_date}'
GROUP BY user_id, loan_provide_no
) a

left outer join

(
select user_id, floor(round(prob, 2) * 100) as prob
from orderrisk.users_inloan_fusionnet_predict_result
where dt = '${model_date}'
) b
on a.user_id = b.user_id;
"

if [ $? -ne 0 ]; then
    printlog "Step 0: 生成订单日前一日模型分数分布和订单日拿去花订单往后的逾期情况失败"
    exit 1
fi
printlog "Step 0: 生成订单日前一日模型分数分布和订单日拿去花订单往后的逾期情况成功"


# Step 1: 生成模型分数段作用于订单表现的聚合信息

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
create table if not exists monitor.users_inloan_fusionnet_order_summary (
prob_level         string        comment '分数段(越高越坏)',

total_orders       bigint        comment '总拿去花订单数',
perf_orders        bigint        comment '到达表现期的订单数',

not_overdue_orders    bigint        comment '(到达表现期中)无逾期订单数',
overdue_orders        bigint        comment '(到达表现期中)有逾期订单数',
overdue_30_orders     bigint        comment '(到达表现期中)逾期30天+订单数',
overdue_60_orders     bigint        comment '(到达表现期中)逾期60天+订单数',
overdue_90_orders     bigint        comment '(到达表现期中)逾期90天+订单数'
)
PARTITIONED BY (dt string comment '订单日期')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

insert overwrite table monitor.users_inloan_fusionnet_order_summary partition (dt = '${order_date}') 

select prob_level,
count(distinct loan_provide_no) as total_orders,
count(distinct if(has_performance = 1, loan_provide_no, null)) as perf_orders,
count(distinct if(has_performance = 1 and overdue_label = 0, loan_provide_no, null)) as not_overdue_orders,
count(distinct if(has_performance = 1 and overdue_label = 1, loan_provide_no, null)) as overdue_orders,
count(distinct if(has_performance = 1 and overdue_30days_label = 1, loan_provide_no, null)) as overdue_30_orders,
count(distinct if(has_performance = 1 and overdue_60days_label = 1, loan_provide_no, null)) as overdue_60_orders,
count(distinct if(has_performance = 1 and overdue_90days_label = 1, loan_provide_no, null)) as overdue_90_orders
from monitor.users_inloan_fusionnet_order_detail
group by prob_level;
"

if [ $? -ne 0 ]; then
    printlog "Step 1: 生成订单日前一日模型分数分布和订单日拿去花订单往后的逾期情况失败"
    exit 1
fi
printlog "Step 1: 生成订单日前一日模型分数分布和订单日拿去花订单往后的逾期情况成功"

# Step 2: 结果导入mysql

monitor_path='/home/q/bizdata/temp/monitor'
if [ ! -d ${monitor_path} ]
then
    mkdir ${monitor_path}
fi

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
select *
from monitor.users_inloan_fusionnet_order_summary where dt = '${order_date}'
" > ${monitor_path}/users_inloan_fusionnet_order_summary

sudo -ubizdata mysql -h192.168.52.62 -uhive -p'Aqas~1234~!@' -P3306 --default-character-set=utf8 -A report --local-infile=1 -e "
set names utf8;

create table if not exists users_inloan_fusionnet_order_summary (
prob_level         varchar(50) not null,

total_orders       int default 0,    
perf_orders        int default 0,          

not_overdue_orders   int default 0, 
overdue_orders       int default 0, 
overdue_30_orders    int default 0, 
overdue_60_orders    int default 0, 
overdue_90_orders    int default 0,
dt                 varchar(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
delete from users_inloan_fusionnet_order_summary where dt = '${order_date}';
load data local infile '${monitor_path}/users_inloan_fusionnet_order_summary' into table users_inloan_fusionnet_order_summary fields terminated by '\t';
"
rm -f ${monitor_path}/users_inloan_fusionnet_order_summary

if [ $? -ne 0 ]; then
    printlog "Step 2: 结果导入mysql失败"
    exit 1
fi
printlog "Step 2: 结果导入mysql成功"

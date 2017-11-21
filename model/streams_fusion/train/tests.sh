#!/bin/bash

# Step 0: 取2017年5月2日下单的贷中用户构建测试集

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE orderrisk.users_inloan_tests;
CREATE TABLE orderrisk.users_inloan_tests as

SELECT a.user_id, a.begin_date, a.end_date, a.label,
case when a.end_date = b.first_order_date then 1
     when a.end_date > b.first_order_date then 0
     else -999 end as is_first_order_day
FROM
(
SELECT user_id, null as begin_date, dt as end_date, label
FROM (
SELECT user_id, dt,
case when count_where(term_label = 1) = count(*) then 1 when count_where(term_label = 0) = count(*) then 0 else -999 end as label
FROM (
SELECT user_id, to_date(order_time) as dt, case when od_days_cnt >= 30 then 1 when od_days_cnt = 0 then 0 else -999 end as term_label
FROM dw_cube.cube_term_repayment
WHERE to_date(order_time) = '2017-05-02' and
      org_channel = 'QUNAR' AND product_no = 'IOUS'
) a
GROUP BY user_id, dt
) final
) a

LEFT OUTER JOIN

(
SELECT user_id, to_date(min(order_time)) as first_order_date
FROM dw_cube.cube_original_order
WHERE org_channel = 'QUNAR' AND product_no = 'IOUS'
GROUP BY user_id
) b
ON a.user_id = b.user_id;
"


# Step 2: 计算用户快照型特征
/home/q/anaconda2/bin/python /home/q/bizdata/data/shell/tools/python/feature_generation/the_moment_dimension/generate_event_feature.py /home/q/bizdata/data/order_risk/streams_fusion/train/users_inloan_tests.json

# Step 3: 计算用户时间序列型
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists orderrisk.users_inloan_tests_month;

CREATE TABLE orderrisk.users_inloan_tests_month as

SELECT user_id, begin_month, end_month
FROM (
SELECT user_id, 
concat_ws('-', cast(year(end_date) - 4 as string), lpad(month(end_date), 2, 0)) as begin_month,
substr(end_date, 1, 7) as end_month
FROM orderrisk.users_inloan_tests
) a
GROUP BY user_id, begin_month, end_month;
"

bash /home/q/bizdata/data/shell/tools/python/feature_generation/time_series/run_ts_features.sh orderrisk.users_inloan_tests_month orderrisk.users_inloan_testset_month

# Step 4: 组合快照型和时间序列型特征
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists orderrisk.users_inloan_fusion_testset;

CREATE TABLE orderrisk.users_inloan_fusion_testset as

SELECT t1.*, t2.features_json
FROM orderrisk.users_inloan_testset t1
JOIN orderrisk.users_inloan_testset_month t2
ON t1.user_id = t2.user_id and substr(t1.end_date, 1, 7) = t2.end_month
WHERE is_not_null(t2.features_json);
"

# Step 5: 关联回来订单，对每个订单进行评分
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists orderrisk.users_inloan_fusion_testset_for_orders;

CREATE TABLE orderrisk.users_inloan_fusion_testset_for_orders as

SELECT a.*, b.loan_provide_no as loan_provide_no, b.order_label as order_label
FROM orderrisk.users_inloan_fusion_testset a

JOIN

(
SELECT user_id, loan_provide_no,
case when max(od_days_cnt) >= 30 then 1 when max(od_days_cnt) = 0 then 0 else -999 end as order_label
FROM dw_cube.cube_term_repayment
WHERE org_channel = 'QUNAR' AND product_no = 'IOUS' AND to_date(order_time) = '2017-05-02'
GROUP BY user_id, loan_provide_no
) b
ON a.user_id = b.user_id;
"

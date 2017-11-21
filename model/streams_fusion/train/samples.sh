#!/bin/bash

# Step 0: 取2017年5月之前下单的贷中用户构建训练/验证集

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE orderrisk.users_inloan_samples;
CREATE TABLE orderrisk.users_inloan_samples as

SELECT a.user_id, a.begin_date, a.end_date, a.label
FROM
(
SELECT user_id, null as begin_date, dt as end_date, label
FROM (
SELECT user_id, dt,
case when count_where(term_label = 1) = count(*) then 1 when count_where(term_label = 0) = count(*) then 0 else -999 end as label
FROM (
SELECT user_id, to_date(order_time) as dt, case when od_days_cnt >= 30 then 1 when od_days_cnt = 0 then 0 else -999 end as term_label
FROM dw_cube.cube_term_repayment
WHERE to_date(order_time) < '2017-05-01' and org_channel = 'QUNAR' AND product_no = 'IOUS'
) a
WHERE term_label in (0, 1)
GROUP BY user_id, dt
) final
WHERE label in (0, 1)
) a

JOIN

(
SELECT user_id, to_date(min(order_time)) as first_order_date
FROM dw_cube.cube_original_order
WHERE org_channel = 'QUNAR' AND product_no = 'IOUS'
GROUP BY user_id
) b
ON a.user_id = b.user_id
WHERE a.end_date > b.first_order_date;
"

# Step 1: 用户集进行分层采样，正负样本各取8W

sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE orderrisk.users_inloan_samples_sampled;
CREATE TABLE orderrisk.users_inloan_samples_sampled as

SELECT user_id, begin_date, end_date, label
FROM (
SELECT user_id, begin_date, end_date, label,
count(*) over (partition by label) as label_cnt,
rank() over (partition by label order by rand()) as label_rank
FROM orderrisk.users_inloan_samples
) a
WHERE label_rank <= 80000;
"

# Step 2: 计算用户快照型特征
/home/q/anaconda2/bin/python /home/q/bizdata/data/shell/tools/python/feature_generation/the_moment_dimension/generate_event_feature.py /home/q/bizdata/data/order_risk/streams_fusion/train/users_inloan.json

# Step 3: 计算用户时间序列型
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists orderrisk.users_inloan_samples_sampled_month;

CREATE TABLE orderrisk.users_inloan_samples_sampled_month as

SELECT user_id, begin_month, end_month
FROM (
SELECT user_id, 
concat_ws('-', cast(year(end_date) - 4 as string), lpad(month(end_date), 2, 0)) as begin_month,
substr(end_date, 1, 7) as end_month
FROM orderrisk.users_inloan_samples_sampled
) a
GROUP BY user_id, begin_month, end_month;
"

bash /home/q/bizdata/data/shell/tools/python/feature_generation/time_series/run_ts_features.sh orderrisk.users_inloan_samples_sampled_month orderrisk.users_inloan_dataset_month

# Step 4: 组合快照型和时间序列型特征
sudo -ubizdata /home/q/hive/hive-0.12.0/bin/hive -e "
DROP TABLE if exists orderrisk.users_inloan_fusion_dataset;

CREATE TABLE orderrisk.users_inloan_fusion_dataset as

SELECT t1.*, t2.features_json
FROM orderrisk.users_inloan_dataset t1
JOIN orderrisk.users_inloan_dataset_month t2
ON t1.user_id = t2.user_id and substr(t1.end_date, 1, 7) = t2.end_month
WHERE is_not_null(t2.features_json);
"

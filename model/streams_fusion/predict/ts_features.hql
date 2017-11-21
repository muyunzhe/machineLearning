set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;

set mapreduce.map.memory.mb=4096;
set mapred.child.map.java.opts=-Xmx3000M;
set mapreduce.map.java.opts=-Xmx3000M;
set mapreduce.reduce.memory.mb=4096;
set mapred.child.reduce.java.opts=-Xmx3600M;
set mapreduce.reduce.java.opts=-Xmx3600M;
set mapreduce.job.reduces=800;

-- user variables
set source_table=orderrisk.users_inloan_fusionnet_predict_user_set_month;
set target_table=orderrisk.users_inloan_fusionnet_predict_ts_features;

add file /home/q/bizdata/data/shell/tools/python/feature_generation/time_series/tsfm_combine_string_to_json.py;
add file /home/q/bizdata/data/shell/tools/python/feature_generation/time_series/tsfm_combine_json_to_json.py;

-- 订单
DROP VIEW if exists tsfm.order_general_features_middle;
DROP VIEW if exists tsfm.order_flight_features_middle;
DROP VIEW if exists tsfm.order_hotel_features_middle;
DROP VIEW if exists tsfm.order_train_features_middle;
DROP VIEW if exists tsfm.order_others_features_middle;

-- 支付 卡库 常旅
DROP VIEW if exists tsfm.pay_features_middle;
DROP VIEW if exists tsfm.bankcard_features_middle;
DROP VIEW if exists tsfm.contact_features_middle;

-- 拿去花
DROP VIEW if exists tsfm.ious_loan_middle;
DROP VIEW if exists tsfm.ious_repay_middle;
DROP VIEW if exists tsfm.ious_refund_middle;

-- 日志
DROP VIEW if exists tsfm.log_user_features_middle;
DROP VIEW if exists tsfm.log_flight_features_middle;
DROP VIEW if exists tsfm.log_train_features_middle;
DROP VIEW if exists tsfm.log_hotel_features_middle;
DROP VIEW if exists tsfm.log_group_features_middle;
DROP VIEW if exists tsfm.log_other_features_middle;
DROP VIEW if exists tsfm.log_unknown_features_middle;

-- 目标写入
DROP TABLE if exists ${hiveconf:target_table};

---------------------------------------------------------------------------------------------------

--Step 2.1: 订单图-通用

CREATE VIEW tsfm.order_general_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_order_amount is null or month_order_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11001'), cast(month_order_amount as string)))
 ) as c11001,
collect_set(if(month_order_count is null or month_order_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11002'), cast(month_order_count as string)))
 ) as c11002,
collect_set(if(month_order_success_amount is null or month_order_success_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11003'), cast(month_order_success_amount as string)))
 ) as c11003,
collect_set(if(month_order_success_count is null or month_order_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11004'), cast(month_order_success_count as string)))
 ) as c11004,
collect_set(if(month_order_cancel_amount is null or month_order_cancel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11005'), cast(month_order_cancel_amount as string)))
 ) as c11005,
collect_set(if(month_order_cancel_count is null or month_order_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11006'), cast(month_order_cancel_count as string)))
 ) as c11006,
collect_set(if(month_order_refund_amount is null or month_order_refund_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11007'), cast(month_order_refund_amount as string)))
 ) as c11007,
collect_set(if(month_order_refund_count is null or month_order_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11008'), cast(month_order_refund_count as string)))
 ) as c11008,
collect_set(if(month_order_app_amount is null or month_order_app_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11009'), cast(month_order_app_amount as string)))
 ) as c11009,
collect_set(if(month_order_app_count is null or month_order_app_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11010'), cast(month_order_app_count as string)))
 ) as c11010,
collect_set(if(month_order_www_amount is null or month_order_www_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11011'), cast(month_order_www_amount as string)))
 ) as c11011,
collect_set(if(month_order_www_count is null or month_order_www_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11012'), cast(month_order_www_count as string)))
 ) as c11012,
collect_set(if(month_order_touch_amount is null or month_order_touch_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '11013'), cast(month_order_touch_amount as string)))
 ) as c11013,
collect_set(if(month_order_touch_count is null or month_order_touch_count = 0, null,
 concat_ws('=', concat_ws('_', month, '11014'), cast(month_order_touch_count as string)))
 ) as c11014,
collect_set(if(month_order_in_daytime is null or month_order_in_daytime = 0, null,
 concat_ws('=', concat_ws('_', month, '11015'), cast(month_order_in_daytime as string)))
 ) as c11015,
collect_set(if(month_order_at_night is null or month_order_at_night = 0, null,
 concat_ws('=', concat_ws('_', month, '11016'), cast(month_order_at_night as string)))
 ) as c11016,
collect_set(if(month_order_avg_hour is null or month_order_avg_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '11017'), cast(month_order_avg_hour as string)))
 ) as c11017

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.oc_order_feature_uid_month
WHERE is_not_null(user_id)
) t1
ON t.user_id = t1.user_id

WHERE 
    t1.month >= t.begin_month and t1.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 2.2: 订单图-机票

CREATE VIEW tsfm.order_flight_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_flight_amount is null or month_flight_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12001'), cast(month_flight_amount as string)))
 ) as c12001,
collect_set(if(month_flight_count is null or month_flight_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12002'), cast(month_flight_count as string)))
 ) as c12002,
collect_set(if(month_flight_success_amount is null or month_flight_success_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12003'), cast(month_flight_success_amount as string)))
 ) as c12003,
collect_set(if(month_flight_success_count is null or month_flight_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12004'), cast(month_flight_success_count as string)))
 ) as c12004,
collect_set(if(month_flight_cancel_amount is null or month_flight_cancel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12005'), cast(month_flight_cancel_amount as string)))
 ) as c12005,
collect_set(if(month_flight_cancel_count is null or month_flight_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12006'), cast(month_flight_cancel_count as string)))
 ) as c12006,
collect_set(if(month_flight_refund_amount is null or month_flight_refund_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12007'), cast(month_flight_refund_amount as string)))
 ) as c12007,
collect_set(if(month_flight_refund_count is null or month_flight_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12008'), cast(month_flight_refund_count as string)))
 ) as c12008,
collect_set(if(month_flight_fc_cabin_amount is null or month_flight_fc_cabin_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12009'), cast(month_flight_fc_cabin_amount as string)))
 ) as c12009,
collect_set(if(month_flight_fc_cabin_count is null or month_flight_fc_cabin_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12010'), cast(month_flight_fc_cabin_count as string)))
 ) as c12010,
collect_set(if(month_flight_abroad_amount is null or month_flight_abroad_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12011'), cast(month_flight_abroad_amount as string)))
 ) as c12011,
collect_set(if(month_flight_abroad_count is null or month_flight_abroad_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12012'), cast(month_flight_abroad_count as string)))
 ) as c12012,
collect_set(if(month_flight_avg_unit_price is null or month_flight_avg_unit_price = 0, null,
 concat_ws('=', concat_ws('_', month, '12013'), cast(month_flight_avg_unit_price as string)))
 ) as c12013,
collect_set(if(month_flight_avg_z_price is null or month_flight_avg_z_price = 0, null,
 concat_ws('=', concat_ws('_', month, '12014'), cast(month_flight_avg_z_price as string)))
 ) as c12014,
collect_set(if(month_flight_need_express_count is null or month_flight_need_express_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12015'), cast(month_flight_need_express_count as string)))
 ) as c12015,
collect_set(if(month_flight_hour is null or month_flight_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '12016'), cast(month_flight_hour as string)))
 ) as c12016,
collect_set(if(month_flight_non_round_trip_amount is null or month_flight_non_round_trip_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12017'), cast(month_flight_non_round_trip_amount as string)))
 ) as c12017,
collect_set(if(month_flight_non_round_trip_count is null or month_flight_non_round_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12018'), cast(month_flight_non_round_trip_count as string)))
 ) as c12018,
collect_set(if(month_flight_round_trip_amount is null or month_flight_round_trip_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '12019'), cast(month_flight_round_trip_amount as string)))
 ) as c12019,
collect_set(if(month_flight_round_trip_count is null or month_flight_round_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12020'), cast(month_flight_round_trip_count as string)))
 ) as c12020,
collect_set(if(month_flight_partner_trip_count is null or month_flight_partner_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12021'), cast(month_flight_partner_trip_count as string)))
 ) as c12021,
collect_set(if(month_flight_long_holiday_trip_count is null or month_flight_long_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12022'), cast(month_flight_long_holiday_trip_count as string)))
 ) as c12022,
collect_set(if(month_flight_short_holiday_trip_count is null or month_flight_short_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12023'), cast(month_flight_short_holiday_trip_count as string)))
 ) as c12023,
collect_set(if(month_flight_weekend_trip_count is null or month_flight_weekend_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12024'), cast(month_flight_weekend_trip_count as string)))
 ) as c12024,
collect_set(if(month_flight_weekday_trip_count is null or month_flight_weekday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12025'), cast(month_flight_weekday_trip_count as string)))
 ) as c12025,
collect_set(if(month_flight_avg_pre_order_day is null or month_flight_avg_pre_order_day = 0, null,
 concat_ws('=', concat_ws('_', month, '12026'), cast(month_flight_avg_pre_order_day as string)))
 ) as c12026,
collect_set(if(month_flight_avg_order_hour is null or month_flight_avg_order_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '12027'), cast(month_flight_avg_order_hour as string)))
 ) as c12027,
collect_set(if(month_flight_red_eye_count is null or month_flight_red_eye_count = 0, null,
 concat_ws('=', concat_ws('_', month, '12028'), cast(month_flight_red_eye_count as string)))
 ) as c12028

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.oc_flight_feature_uid_month
WHERE is_not_null(user_id)
) t2
ON t.user_id = t2.user_id

WHERE 
    t2.month >= t.begin_month and t2.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 2.3: 订单图-酒店

CREATE VIEW tsfm.order_hotel_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_hotel_amount is null or month_hotel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13001'), cast(month_hotel_amount as string)))
 ) as c13001,
collect_set(if(month_hotel_count is null or month_hotel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13002'), cast(month_hotel_count as string)))
 ) as c13002,
collect_set(if(month_hotel_success_amount is null or month_hotel_success_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13003'), cast(month_hotel_success_amount as string)))
 ) as c13003,
collect_set(if(month_hotel_success_count is null or month_hotel_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13004'), cast(month_hotel_success_count as string)))
 ) as c13004,
collect_set(if(month_hotel_cancel_amount is null or month_hotel_cancel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13005'), cast(month_hotel_cancel_amount as string)))
 ) as c13005,
collect_set(if(month_hotel_cancel_count is null or month_hotel_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13006'), cast(month_hotel_cancel_count as string)))
 ) as c13006,
collect_set(if(month_hotel_refund_amount is null or month_hotel_refund_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13007'), cast(month_hotel_refund_amount as string)))
 ) as c13007,
collect_set(if(month_hotel_refund_count is null or month_hotel_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13008'), cast(month_hotel_refund_count as string)))
 ) as c13008,
collect_set(if(month_hotel_noshow_amount is null or month_hotel_noshow_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13009'), cast(month_hotel_noshow_amount as string)))
 ) as c13009,
collect_set(if(month_hotel_noshow_count is null or month_hotel_noshow_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13010'), cast(month_hotel_noshow_count as string)))
 ) as c13010,
collect_set(if(month_hotel_high_star_amount is null or month_hotel_high_star_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13011'), cast(month_hotel_high_star_amount as string)))
 ) as c13011,
collect_set(if(month_hotel_high_star_count is null or month_hotel_high_star_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13012'), cast(month_hotel_high_star_count as string)))
 ) as c13012,
collect_set(if(month_hotel_high_grade_amount is null or month_hotel_high_grade_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13013'), cast(month_hotel_high_grade_amount as string)))
 ) as c13013,
collect_set(if(month_hotel_high_grade_count is null or month_hotel_high_grade_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13014'), cast(month_hotel_high_grade_count as string)))
 ) as c13014,
collect_set(if(month_hotel_abroad_amount is null or month_hotel_abroad_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '13015'), cast(month_hotel_abroad_amount as string)))
 ) as c13015,
collect_set(if(month_hotel_abroad_count is null or month_hotel_abroad_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13016'), cast(month_hotel_abroad_count as string)))
 ) as c13016,
collect_set(if(month_hotel_avg_unit_price is null or month_hotel_avg_unit_price = 0, null,
 concat_ws('=', concat_ws('_', month, '13017'), cast(month_hotel_avg_unit_price as string)))
 ) as c13017,
collect_set(if(month_hotel_avg_z_price is null or month_hotel_avg_z_price = 0, null,
 concat_ws('=', concat_ws('_', month, '13018'), cast(month_hotel_avg_z_price as string)))
 ) as c13018,
collect_set(if(month_hotel_need_bill_count is null or month_hotel_need_bill_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13019'), cast(month_hotel_need_bill_count as string)))
 ) as c13019,
collect_set(if(month_hotel_room_nights is null or month_hotel_room_nights = 0, null,
 concat_ws('=', concat_ws('_', month, '13020'), cast(month_hotel_room_nights as string)))
 ) as c13020,
collect_set(if(month_hotel_pre_pay_count is null or month_hotel_pre_pay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13021'), cast(month_hotel_pre_pay_count as string)))
 ) as c13021,
collect_set(if(month_hotel_post_pay_count is null or month_hotel_post_pay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13022'), cast(month_hotel_post_pay_count as string)))
 ) as c13022,
collect_set(if(month_hotel_bigbed_room_count is null or month_hotel_bigbed_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13023'), cast(month_hotel_bigbed_room_count as string)))
 ) as c13023,
collect_set(if(month_hotel_double_room_count is null or month_hotel_double_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13024'), cast(month_hotel_double_room_count as string)))
 ) as c13024,
collect_set(if(month_hotel_single_room_count is null or month_hotel_single_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13025'), cast(month_hotel_single_room_count as string)))
 ) as c13025,
collect_set(if(month_hotel_theme_room_count is null or month_hotel_theme_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13026'), cast(month_hotel_theme_room_count as string)))
 ) as c13026,
collect_set(if(month_hotel_triple_room_count is null or month_hotel_triple_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13027'), cast(month_hotel_triple_room_count as string)))
 ) as c13027,
collect_set(if(month_hotel_multi_room_count is null or month_hotel_multi_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13028'), cast(month_hotel_multi_room_count as string)))
 ) as c13028,
collect_set(if(month_hotel_family_room_count is null or month_hotel_family_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13029'), cast(month_hotel_family_room_count as string)))
 ) as c13029,
collect_set(if(month_hotel_suite_room_count is null or month_hotel_suite_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13030'), cast(month_hotel_suite_room_count as string)))
 ) as c13030,
collect_set(if(month_hotel_view_room_count is null or month_hotel_view_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13031'), cast(month_hotel_view_room_count as string)))
 ) as c13031,
collect_set(if(month_hotel_busi_room_count is null or month_hotel_busi_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13032'), cast(month_hotel_busi_room_count as string)))
 ) as c13032,
collect_set(if(month_hotel_cheap_room_count is null or month_hotel_cheap_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13033'), cast(month_hotel_cheap_room_count as string)))
 ) as c13033,
collect_set(if(month_hotel_normal_room_count is null or month_hotel_normal_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13034'), cast(month_hotel_normal_room_count as string)))
 ) as c13034,
collect_set(if(month_hotel_super_room_count is null or month_hotel_super_room_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13035'), cast(month_hotel_super_room_count as string)))
 ) as c13035,
collect_set(if(month_hotel_long_holiday_trip_count is null or month_hotel_long_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13036'), cast(month_hotel_long_holiday_trip_count as string)))
 ) as c13036,
collect_set(if(month_hotel_short_holiday_trip_count is null or month_hotel_short_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13037'), cast(month_hotel_short_holiday_trip_count as string)))
 ) as c13037,
collect_set(if(month_hotel_weekend_trip_count is null or month_hotel_weekend_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13038'), cast(month_hotel_weekend_trip_count as string)))
 ) as c13038,
collect_set(if(month_hotel_weekday_trip_count is null or month_hotel_weekday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '13039'), cast(month_hotel_weekday_trip_count as string)))
 ) as c13039,
collect_set(if(month_hotel_avg_pre_order_day is null or month_hotel_avg_pre_order_day = 0, null,
 concat_ws('=', concat_ws('_', month, '13040'), cast(month_hotel_avg_pre_order_day as string)))
 ) as c13040,
collect_set(if(month_hotel_avg_order_hour is null or month_hotel_avg_order_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '13041'), cast(month_hotel_avg_order_hour as string)))
 ) as c13041

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.oc_hotel_feature_uid_month
WHERE is_not_null(user_id)
) t3
ON t.user_id = t3.user_id

WHERE 
    t3.month >= t.begin_month and t3.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 2.4: 订单图-火车

CREATE VIEW tsfm.order_train_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_train_amount is null or month_train_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '14001'), cast(month_train_amount as string)))
 ) as c14001,
collect_set(if(month_train_count is null or month_train_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14002'), cast(month_train_count as string)))
 ) as c14002,
collect_set(if(month_train_success_amount is null or month_train_success_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '14003'), cast(month_train_success_amount as string)))
 ) as c14003,
collect_set(if(month_train_success_count is null or month_train_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14004'), cast(month_train_success_count as string)))
 ) as c14004,
collect_set(if(month_train_cancel_amount is null or month_train_cancel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '14005'), cast(month_train_cancel_amount as string)))
 ) as c14005,
collect_set(if(month_train_cancel_count is null or month_train_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14006'), cast(month_train_cancel_count as string)))
 ) as c14006,
collect_set(if(month_train_refund_amount is null or month_train_refund_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '14007'), cast(month_train_refund_amount as string)))
 ) as c14007,
collect_set(if(month_train_refund_count is null or month_train_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14008'), cast(month_train_refund_count as string)))
 ) as c14008,
collect_set(if(month_train_crh_amount is null or month_train_crh_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '14009'), cast(month_train_crh_amount as string)))
 ) as c14009,
collect_set(if(month_train_crh_count is null or month_train_crh_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14010'), cast(month_train_crh_count as string)))
 ) as c14010,
collect_set(if(month_train_long_trip_crh_amount is null or month_train_long_trip_crh_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '14011'), cast(month_train_long_trip_crh_amount as string)))
 ) as c14011,
collect_set(if(month_train_long_trip_crh_count is null or month_train_long_trip_crh_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14012'), cast(month_train_long_trip_crh_count as string)))
 ) as c14012,
collect_set(if(month_train_avg_unit_price is null or month_train_avg_unit_price = 0, null,
 concat_ws('=', concat_ws('_', month, '14013'), cast(month_train_avg_unit_price as string)))
 ) as c14013,
collect_set(if(month_train_partner_trip_count is null or month_train_partner_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14014'), cast(month_train_partner_trip_count as string)))
 ) as c14014,
collect_set(if(month_train_need_insurance_count is null or month_train_need_insurance_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14015'), cast(month_train_need_insurance_count as string)))
 ) as c14015,
collect_set(if(month_train_train_hour is null or month_train_train_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '14016'), cast(month_train_train_hour as string)))
 ) as c14016,
collect_set(if(month_train_long_holiday_trip_count is null or month_train_long_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14017'), cast(month_train_long_holiday_trip_count as string)))
 ) as c14017,
collect_set(if(month_train_short_holiday_trip_count is null or month_train_short_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14018'), cast(month_train_short_holiday_trip_count as string)))
 ) as c14018,
collect_set(if(month_train_weekend_trip_count is null or month_train_weekend_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14019'), cast(month_train_weekend_trip_count as string)))
 ) as c14019,
collect_set(if(month_train_weekday_trip_count is null or month_train_weekday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '14020'), cast(month_train_weekday_trip_count as string)))
 ) as c14020,
collect_set(if(month_train_avg_pre_order_day is null or month_train_avg_pre_order_day = 0, null,
 concat_ws('=', concat_ws('_', month, '14021'), cast(month_train_avg_pre_order_day as string)))
 ) as c14021,
collect_set(if(month_train_avg_order_hour is null or month_train_avg_order_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '14022'), cast(month_train_avg_order_hour as string)))
 ) as c14022

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.oc_train_feature_uid_month
WHERE is_not_null(user_id)
) t4
ON t.user_id = t4.user_id

WHERE 
    t4.month >= t.begin_month and t4.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 2.5: 订单图-其他(度假 + 门票)

CREATE VIEW tsfm.order_others_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_others_amount is null or month_others_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '15001'), cast(month_others_amount as string)))
 ) as c15001,
collect_set(if(month_others_count is null or month_others_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15002'), cast(month_others_count as string)))
 ) as c15002,
collect_set(if(month_others_success_amount is null or month_others_success_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '15003'), cast(month_others_success_amount as string)))
 ) as c15003,
collect_set(if(month_others_success_count is null or month_others_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15004'), cast(month_others_success_count as string)))
 ) as c15004,
collect_set(if(month_others_cancel_amount is null or month_others_cancel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '15005'), cast(month_others_cancel_amount as string)))
 ) as c15005,
collect_set(if(month_others_cancel_count is null or month_others_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15006'), cast(month_others_cancel_count as string)))
 ) as c15006,
collect_set(if(month_others_refund_amount is null or month_others_refund_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '15007'), cast(month_others_refund_amount as string)))
 ) as c15007,
collect_set(if(month_others_refund_count is null or month_others_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15008'), cast(month_others_refund_count as string)))
 ) as c15008,
collect_set(if(month_holiday_abroad_amount is null or month_holiday_abroad_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '15009'), cast(month_holiday_abroad_amount as string)))
 ) as c15009,
collect_set(if(month_holiday_abroad_count is null or month_holiday_abroad_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15010'), cast(month_holiday_abroad_count as string)))
 ) as c15010,
collect_set(if(month_others_avg_unit_price is null or month_others_avg_unit_price = 0, null,
 concat_ws('=', concat_ws('_', month, '15011'), cast(month_others_avg_unit_price as string)))
 ) as c15011,
collect_set(if(month_holiday_day_amount is null or month_holiday_day_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '15012'), cast(month_holiday_day_amount as string)))
 ) as c15012,
collect_set(if(month_holiday_avg_day is null or month_holiday_avg_day = 0, null,
 concat_ws('=', concat_ws('_', month, '15013'), cast(month_holiday_avg_day as string)))
 ) as c15013,
collect_set(if(month_holiday_with_child_count is null or month_holiday_with_child_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15014'), cast(month_holiday_with_child_count as string)))
 ) as c15014,
collect_set(if(month_others_single_count is null or month_others_single_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15015'), cast(month_others_single_count as string)))
 ) as c15015,
collect_set(if(month_others_pair_count is null or month_others_pair_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15016'), cast(month_others_pair_count as string)))
 ) as c15016,
collect_set(if(month_others_multi_player_count is null or month_others_multi_player_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15017'), cast(month_others_multi_player_count as string)))
 ) as c15017,
collect_set(if(month_holiday_date_product_count is null or month_holiday_date_product_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15018'), cast(month_holiday_date_product_count as string)))
 ) as c15018,
collect_set(if(month_holiday_type_product_count is null or month_holiday_type_product_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15019'), cast(month_holiday_type_product_count as string)))
 ) as c15019,
collect_set(if(month_holiday_long_holiday_trip_count is null or month_holiday_long_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15020'), cast(month_holiday_long_holiday_trip_count as string)))
 ) as c15020,
collect_set(if(month_holiday_short_holiday_trip_count is null or month_holiday_short_holiday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15021'), cast(month_holiday_short_holiday_trip_count as string)))
 ) as c15021,
collect_set(if(month_holiday_weekend_trip_count is null or month_holiday_weekend_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15022'), cast(month_holiday_weekend_trip_count as string)))
 ) as c15022,
collect_set(if(month_holiday_weekday_trip_count is null or month_holiday_weekday_trip_count = 0, null,
 concat_ws('=', concat_ws('_', month, '15023'), cast(month_holiday_weekday_trip_count as string)))
 ) as c15023,
collect_set(if(month_holiday_avg_pre_order_day is null or month_holiday_avg_pre_order_day = 0, null,
 concat_ws('=', concat_ws('_', month, '15024'), cast(month_holiday_avg_pre_order_day as string)))
 ) as c15024,
collect_set(if(month_others_avg_order_hour is null or month_others_avg_order_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '15025'), cast(month_others_avg_order_hour as string)))
 ) as c15025

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.oc_others_feature_uid_month
WHERE is_not_null(user_id)
) t5
ON t.user_id = t5.user_id

WHERE 
    t5.month >= t.begin_month and t5.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

----------------------------------------------------------------------------------------------------

-- Step 3.1: 支付*绑卡*常旅客图-支付

CREATE VIEW tsfm.pay_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_pay_amount is null or month_pay_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21001'), cast(month_pay_amount as string)))
 ) as c21001,
collect_set(if(month_pay_count is null or month_pay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21002'), cast(month_pay_count as string)))
 ) as c21002,
collect_set(if(month_pay_success_amount is null or month_pay_success_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21003'), cast(month_pay_success_amount as string)))
 ) as c21003,
collect_set(if(month_pay_success_count is null or month_pay_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21004'), cast(month_pay_success_count as string)))
 ) as c21004,
collect_set(if(month_pay_cancel_amount is null or month_pay_cancel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21005'), cast(month_pay_cancel_amount as string)))
 ) as c21005,
collect_set(if(month_pay_cancel_count is null or month_pay_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21006'), cast(month_pay_cancel_count as string)))
 ) as c21006,
collect_set(if(month_pay_refund_amount is null or month_pay_refund_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21007'), cast(month_pay_refund_amount as string)))
 ) as c21007,
collect_set(if(month_pay_refund_count is null or month_pay_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21008'), cast(month_pay_refund_count as string)))
 ) as c21008,
collect_set(if(month_pay_failed_amount is null or month_pay_failed_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21009'), cast(month_pay_failed_amount as string)))
 ) as c21009,
collect_set(if(month_pay_failed_count is null or month_pay_failed_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21010'), cast(month_pay_failed_count as string)))
 ) as c21010,
collect_set(if(month_pay_credit_card_amount is null or month_pay_credit_card_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21011'), cast(month_pay_credit_card_amount as string)))
 ) as c21011,
collect_set(if(month_pay_credit_card_count is null or month_pay_credit_card_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21012'), cast(month_pay_credit_card_count as string)))
 ) as c21012,
collect_set(if(month_pay_debit_card_amount is null or month_pay_debit_card_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21013'), cast(month_pay_debit_card_amount as string)))
 ) as c21013,
collect_set(if(month_pay_debit_card_count is null or month_pay_debit_card_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21014'), cast(month_pay_debit_card_count as string)))
 ) as c21014,
collect_set(if(month_pay_bank_amount is null or month_pay_bank_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21015'), cast(month_pay_bank_amount as string)))
 ) as c21015,
collect_set(if(month_pay_bank_count is null or month_pay_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21016'), cast(month_pay_bank_count as string)))
 ) as c21016,
collect_set(if(month_pay_ious_amount is null or month_pay_ious_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21017'), cast(month_pay_ious_amount as string)))
 ) as c21017,
collect_set(if(month_pay_ious_count is null or month_pay_ious_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21018'), cast(month_pay_ious_count as string)))
 ) as c21018,
collect_set(if(month_pay_lijian_amount is null or month_pay_lijian_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21019'), cast(month_pay_lijian_amount as string)))
 ) as c21019,
collect_set(if(month_pay_lijian_count is null or month_pay_lijian_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21020'), cast(month_pay_lijian_count as string)))
 ) as c21020,
collect_set(if(month_pay_hongbao_amount is null or month_pay_hongbao_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21021'), cast(month_pay_hongbao_amount as string)))
 ) as c21021,
collect_set(if(month_pay_hongbao_count is null or month_pay_hongbao_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21022'), cast(month_pay_hongbao_count as string)))
 ) as c21022,
collect_set(if(month_pay_giftcard_amount is null or month_pay_giftcard_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21023'), cast(month_pay_giftcard_amount as string)))
 ) as c21023,
collect_set(if(month_pay_giftcard_count is null or month_pay_giftcard_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21024'), cast(month_pay_giftcard_count as string)))
 ) as c21024,
collect_set(if(month_pay_balance_amount is null or month_pay_balance_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21025'), cast(month_pay_balance_amount as string)))
 ) as c21025,
collect_set(if(month_pay_balance_count is null or month_pay_balance_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21026'), cast(month_pay_balance_count as string)))
 ) as c21026,
collect_set(if(month_pay_return_amount is null or month_pay_return_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21027'), cast(month_pay_return_amount as string)))
 ) as c21027,
collect_set(if(month_pay_return_count is null or month_pay_return_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21028'), cast(month_pay_return_count as string)))
 ) as c21028,
collect_set(if(month_pay_nocard_amount is null or month_pay_nocard_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21029'), cast(month_pay_nocard_amount as string)))
 ) as c21029,
collect_set(if(month_pay_nocard_count is null or month_pay_nocard_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21030'), cast(month_pay_nocard_count as string)))
 ) as c21030,
collect_set(if(month_pay_thirdparty_amount is null or month_pay_thirdparty_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21031'), cast(month_pay_thirdparty_amount as string)))
 ) as c21031,
collect_set(if(month_pay_thirdparty_count is null or month_pay_thirdparty_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21032'), cast(month_pay_thirdparty_count as string)))
 ) as c21032,
collect_set(if(month_pay_precredit_amount is null or month_pay_precredit_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21033'), cast(month_pay_precredit_amount as string)))
 ) as c21033,
collect_set(if(month_pay_precredit_count is null or month_pay_precredit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21034'), cast(month_pay_precredit_count as string)))
 ) as c21034,
collect_set(if(month_pay_quick_amount is null or month_pay_quick_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21035'), cast(month_pay_quick_amount as string)))
 ) as c21035,
collect_set(if(month_pay_quick_count is null or month_pay_quick_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21036'), cast(month_pay_quick_count as string)))
 ) as c21036,
collect_set(if(month_pay_withhold_amount is null or month_pay_withhold_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21037'), cast(month_pay_withhold_amount as string)))
 ) as c21037,
collect_set(if(month_pay_withhold_count is null or month_pay_withhold_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21038'), cast(month_pay_withhold_count as string)))
 ) as c21038,
collect_set(if(month_pay_apple_amount is null or month_pay_apple_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21039'), cast(month_pay_apple_amount as string)))
 ) as c21039,
collect_set(if(month_pay_apple_count is null or month_pay_apple_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21040'), cast(month_pay_apple_count as string)))
 ) as c21040,
collect_set(if(month_pay_pc_amount is null or month_pay_pc_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21041'), cast(month_pay_pc_amount as string)))
 ) as c21041,
collect_set(if(month_pay_pc_count is null or month_pay_pc_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21042'), cast(month_pay_pc_count as string)))
 ) as c21042,
collect_set(if(month_pay_wap_amount is null or month_pay_wap_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '21043'), cast(month_pay_wap_amount as string)))
 ) as c21043,
collect_set(if(month_pay_wap_count is null or month_pay_wap_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21044'), cast(month_pay_wap_count as string)))
 ) as c21044,
collect_set(if(month_pay_not_enough_balance_count is null or month_pay_not_enough_balance_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21045'), cast(month_pay_not_enough_balance_count as string)))
 ) as c21045,
collect_set(if(month_pay_error_info_count is null or month_pay_error_info_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21046'), cast(month_pay_error_info_count as string)))
 ) as c21046,
collect_set(if(month_pay_invalid_card_count is null or month_pay_invalid_card_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21047'), cast(month_pay_invalid_card_count as string)))
 ) as c21047,
collect_set(if(month_pay_over_limit_count is null or month_pay_over_limit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21048'), cast(month_pay_over_limit_count as string)))
 ) as c21048,
collect_set(if(month_pay_invalid_serve_time_count is null or month_pay_invalid_serve_time_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21049'), cast(month_pay_invalid_serve_time_count as string)))
 ) as c21049,
collect_set(if(month_pay_bank_refuse_count is null or month_pay_bank_refuse_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21050'), cast(month_pay_bank_refuse_count as string)))
 ) as c21050,
collect_set(if(month_pay_overtime_cancel_count is null or month_pay_overtime_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21051'), cast(month_pay_overtime_cancel_count as string)))
 ) as c21051,
collect_set(if(month_pay_busi_cancel_count is null or month_pay_busi_cancel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '21052'), cast(month_pay_busi_cancel_count as string)))
 ) as c21052,
collect_set(if(month_pay_avg_postpay_minute is null or month_pay_avg_postpay_minute = 0, null,
 concat_ws('=', concat_ws('_', month, '21053'), cast(month_pay_avg_postpay_minute as string)))
 ) as c21053,
collect_set(if(month_pay_avg_hour is null or month_pay_avg_hour = 0, null,
 concat_ws('=', concat_ws('_', month, '21054'), cast(month_pay_avg_hour as string)))
 ) as c21054

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.pc_payinfo_feature_uid_month
WHERE is_not_null(user_id)
) t6
ON t.user_id = t6.user_id

WHERE
    t6.month >= t.begin_month and t6.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 3.2: 支付*绑卡*常旅客图-绑卡

CREATE VIEW tsfm.bankcard_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_card_bind_count is null or month_card_bind_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22001'), cast(month_card_bind_count as string)))
 ) as c22001,
collect_set(if(month_card_unbind_count is null or month_card_unbind_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22002'), cast(month_card_unbind_count as string)))
 ) as c22002,
collect_set(if(month_card_debit_count is null or month_card_debit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22003'), cast(month_card_debit_count as string)))
 ) as c22003,
collect_set(if(month_card_credit_count is null or month_card_credit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22004'), cast(month_card_credit_count as string)))
 ) as c22004,
collect_set(if(month_card_normal_count is null or month_card_normal_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22005'), cast(month_card_normal_count as string)))
 ) as c22005,
collect_set(if(month_card_gold_count is null or month_card_gold_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22006'), cast(month_card_gold_count as string)))
 ) as c22006,
collect_set(if(month_card_pt_count is null or month_card_pt_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22007'), cast(month_card_pt_count as string)))
 ) as c22007,
collect_set(if(month_card_normal_debit_count is null or month_card_normal_debit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22008'), cast(month_card_normal_debit_count as string)))
 ) as c22008,
collect_set(if(month_card_gold_debit_count is null or month_card_gold_debit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22009'), cast(month_card_gold_debit_count as string)))
 ) as c22009,
collect_set(if(month_card_pt_debit_count is null or month_card_pt_debit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22010'), cast(month_card_pt_debit_count as string)))
 ) as c22010,
collect_set(if(month_card_normal_credit_count is null or month_card_normal_credit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22011'), cast(month_card_normal_credit_count as string)))
 ) as c22011,
collect_set(if(month_card_gold_credit_count is null or month_card_gold_credit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22012'), cast(month_card_gold_credit_count as string)))
 ) as c22012,
collect_set(if(month_card_pt_credit_count is null or month_card_pt_credit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22013'), cast(month_card_pt_credit_count as string)))
 ) as c22013,
collect_set(if(month_card_big_bank_count is null or month_card_big_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22014'), cast(month_card_big_bank_count as string)))
 ) as c22014,
collect_set(if(month_card_stock_bank_count is null or month_card_stock_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22015'), cast(month_card_stock_bank_count as string)))
 ) as c22015,
collect_set(if(month_card_local_bank_count is null or month_card_local_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22016'), cast(month_card_local_bank_count as string)))
 ) as c22016,
collect_set(if(month_card_foreign_bank_count is null or month_card_foreign_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22017'), cast(month_card_foreign_bank_count as string)))
 ) as c22017,
collect_set(if(month_card_bank_count is null or month_card_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22018'), cast(month_card_bank_count as string)))
 ) as c22018,
collect_set(if(month_card_debit_bank_count is null or month_card_debit_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22019'), cast(month_card_debit_bank_count as string)))
 ) as c22019,
collect_set(if(month_card_credit_bank_count is null or month_card_credit_bank_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22020'), cast(month_card_credit_bank_count as string)))
 ) as c22020,
collect_set(if(month_card_pc_count is null or month_card_pc_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22021'), cast(month_card_pc_count as string)))
 ) as c22021,
collect_set(if(month_card_wap_count is null or month_card_wap_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22022'), cast(month_card_wap_count as string)))
 ) as c22022,
collect_set(if(month_card_pay_bind_count is null or month_card_pay_bind_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22023'), cast(month_card_pay_bind_count as string)))
 ) as c22023,
collect_set(if(month_card_auth_bind_count is null or month_card_auth_bind_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22024'), cast(month_card_auth_bind_count as string)))
 ) as c22024,
collect_set(if(month_card_withdraw_bind_count is null or month_card_withdraw_bind_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22025'), cast(month_card_withdraw_bind_count as string)))
 ) as c22025,
collect_set(if(month_card_self_bind_count is null or month_card_self_bind_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22026'), cast(month_card_self_bind_count as string)))
 ) as c22026,
collect_set(if(month_card_default_count is null or month_card_default_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22027'), cast(month_card_default_count as string)))
 ) as c22027,
collect_set(if(month_card_default_debit_count is null or month_card_default_debit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22028'), cast(month_card_default_debit_count as string)))
 ) as c22028,
collect_set(if(month_card_default_credit_count is null or month_card_default_credit_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22029'), cast(month_card_default_credit_count as string)))
 ) as c22029,
collect_set(if(month_card_mobile_count is null or month_card_mobile_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22030'), cast(month_card_mobile_count as string)))
 ) as c22030,
collect_set(if(month_card_holder_count is null or month_card_holder_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22031'), cast(month_card_holder_count as string)))
 ) as c22031,
collect_set(if(month_card_idcode_count is null or month_card_idcode_count = 0, null,
 concat_ws('=', concat_ws('_', month, '22032'), cast(month_card_idcode_count as string)))
 ) as c22032

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.pc_card_library_feature_uid_month
WHERE is_not_null(user_id)
) t7
ON t.user_id = t7.user_id

WHERE
    t7.month >= t.begin_month and t7.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 3.3: 支付*绑卡*常旅客图-常旅

CREATE VIEW tsfm.contact_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_contact_count is null or month_contact_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23001'), cast(month_contact_count as string)))
 ) as c23001,
collect_set(if(month_contact_male_count is null or month_contact_male_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23002'), cast(month_contact_male_count as string)))
 ) as c23002,
collect_set(if(month_contact_female_count is null or month_contact_female_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23003'), cast(month_contact_female_count as string)))
 ) as c23003,
collect_set(if(month_contact_avg_age is null or month_contact_avg_age = 0, null,
 concat_ws('=', concat_ws('_', month, '23004'), cast(month_contact_avg_age as string)))
 ) as c23004,
collect_set(if(month_contact_unknown_type_count is null or month_contact_unknown_type_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23005'), cast(month_contact_unknown_type_count as string)))
 ) as c23005,
collect_set(if(month_contact_adult_count is null or month_contact_adult_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23006'), cast(month_contact_adult_count as string)))
 ) as c23006,
collect_set(if(month_contact_child_count is null or month_contact_child_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23007'), cast(month_contact_child_count as string)))
 ) as c23007,
collect_set(if(month_contact_baby_count is null or month_contact_baby_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23008'), cast(month_contact_baby_count as string)))
 ) as c23008,
collect_set(if(month_contact_student_count is null or month_contact_student_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23009'), cast(month_contact_student_count as string)))
 ) as c23009,
collect_set(if(month_contact_unknown_source_count is null or month_contact_unknown_source_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23010'), cast(month_contact_unknown_source_count as string)))
 ) as c23010,
collect_set(if(month_contact_flight_count is null or month_contact_flight_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23011'), cast(month_contact_flight_count as string)))
 ) as c23011,
collect_set(if(month_contact_hotel_count is null or month_contact_hotel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23012'), cast(month_contact_hotel_count as string)))
 ) as c23012,
collect_set(if(month_contact_train_count is null or month_contact_train_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23013'), cast(month_contact_train_count as string)))
 ) as c23013,
collect_set(if(month_contact_group_count is null or month_contact_group_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23014'), cast(month_contact_group_count as string)))
 ) as c23014,
collect_set(if(month_contact_uc_count is null or month_contact_uc_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23015'), cast(month_contact_uc_count as string)))
 ) as c23015,
collect_set(if(month_contact_holiday_count is null or month_contact_holiday_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23016'), cast(month_contact_holiday_count as string)))
 ) as c23016,
collect_set(if(month_contact_ticket_count is null or month_contact_ticket_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23017'), cast(month_contact_ticket_count as string)))
 ) as c23017,
collect_set(if(month_contact_cc_count is null or month_contact_cc_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23018'), cast(month_contact_cc_count as string)))
 ) as c23018,
collect_set(if(month_contact_apartment_count is null or month_contact_apartment_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23019'), cast(month_contact_apartment_count as string)))
 ) as c23019,
collect_set(if(month_contact_bus_count is null or month_contact_bus_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23020'), cast(month_contact_bus_count as string)))
 ) as c23020,
collect_set(if(month_contact_passport_count is null or month_contact_passport_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23021'), cast(month_contact_passport_count as string)))
 ) as c23021,
collect_set(if(month_contact_hk_passport_count is null or month_contact_hk_passport_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23022'), cast(month_contact_hk_passport_count as string)))
 ) as c23022,
collect_set(if(month_contact_hk_mc_tw_count is null or month_contact_hk_mc_tw_count = 0, null,
 concat_ws('=', concat_ws('_', month, '23023'), cast(month_contact_hk_mc_tw_count as string)))
 ) as c23023

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.uc_contact_feature_uid_month
WHERE is_not_null(user_id)
) t8
ON t.user_id = t8.user_id
WHERE
    t8.month >= t.begin_month and t8.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

----------------------------------------------------------------------------------------------------

-- Step 4.1: 日志图-用户

CREATE VIEW tsfm.log_user_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_u_log_count is null or month_log_u_log_count = 0, null,
 concat_ws('=', concat_ws('_', month, '40001'), cast(month_log_u_log_count as string)))
 ) as c40001,
collect_set(if(month_log_u_token_reg is null or month_log_u_token_reg = 0, null,
 concat_ws('=', concat_ws('_', month, '40002'), cast(month_log_u_token_reg as string)))
 ) as c40002,
collect_set(if(month_log_u_quick_reg is null or month_log_u_quick_reg = 0, null,
 concat_ws('=', concat_ws('_', month, '40003'), cast(month_log_u_quick_reg as string)))
 ) as c40003,
collect_set(if(month_log_u_spwd_reg is null or month_log_u_spwd_reg = 0, null,
 concat_ws('=', concat_ws('_', month, '40004'), cast(month_log_u_spwd_reg as string)))
 ) as c40004,
collect_set(if(month_log_u_check_phone_reg is null or month_log_u_check_phone_reg = 0, null,
 concat_ws('=', concat_ws('_', month, '40005'), cast(month_log_u_check_phone_reg as string)))
 ) as c40005,
collect_set(if(month_log_u_reg_send_vcode is null or month_log_u_reg_send_vcode = 0, null,
 concat_ws('=', concat_ws('_', month, '40006'), cast(month_log_u_reg_send_vcode as string)))
 ) as c40006,
collect_set(if(month_log_u_add_phone_book is null or month_log_u_add_phone_book = 0, null,
 concat_ws('=', concat_ws('_', month, '40007'), cast(month_log_u_add_phone_book as string)))
 ) as c40007,
collect_set(if(month_log_u_add_contact is null or month_log_u_add_contact = 0, null,
 concat_ws('=', concat_ws('_', month, '40008'), cast(month_log_u_add_contact as string)))
 ) as c40008,
collect_set(if(month_log_u_add_contact_2 is null or month_log_u_add_contact_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40009'), cast(month_log_u_add_contact_2 as string)))
 ) as c40009,
collect_set(if(month_log_u_list_contact is null or month_log_u_list_contact = 0, null,
 concat_ws('=', concat_ws('_', month, '40010'), cast(month_log_u_list_contact as string)))
 ) as c40010,
collect_set(if(month_log_u_list_contact_2 is null or month_log_u_list_contact_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40011'), cast(month_log_u_list_contact_2 as string)))
 ) as c40011,
collect_set(if(month_log_u_del_contact is null or month_log_u_del_contact = 0, null,
 concat_ws('=', concat_ws('_', month, '40012'), cast(month_log_u_del_contact as string)))
 ) as c40012,
collect_set(if(month_log_u_share_order is null or month_log_u_share_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40013'), cast(month_log_u_share_order as string)))
 ) as c40013,
collect_set(if(month_log_u_list_order is null or month_log_u_list_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40014'), cast(month_log_u_list_order as string)))
 ) as c40014,
collect_set(if(month_log_u_order_card is null or month_log_u_order_card = 0, null,
 concat_ws('=', concat_ws('_', month, '40015'), cast(month_log_u_order_card as string)))
 ) as c40015,
collect_set(if(month_log_u_order_card_2 is null or month_log_u_order_card_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40016'), cast(month_log_u_order_card_2 as string)))
 ) as c40016,
collect_set(if(month_log_u_order_card_3 is null or month_log_u_order_card_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40017'), cast(month_log_u_order_card_3 as string)))
 ) as c40017,
collect_set(if(month_log_u_local_order_detail is null or month_log_u_local_order_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40018'), cast(month_log_u_local_order_detail as string)))
 ) as c40018,
collect_set(if(month_log_u_order_detail is null or month_log_u_order_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40019'), cast(month_log_u_order_detail as string)))
 ) as c40019,
collect_set(if(month_log_u_order_detail_2 is null or month_log_u_order_detail_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40020'), cast(month_log_u_order_detail_2 as string)))
 ) as c40020,
collect_set(if(month_log_u_order_card_remind is null or month_log_u_order_card_remind = 0, null,
 concat_ws('=', concat_ws('_', month, '40021'), cast(month_log_u_order_card_remind as string)))
 ) as c40021,
collect_set(if(month_log_u_spwd_check_info is null or month_log_u_spwd_check_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40022'), cast(month_log_u_spwd_check_info as string)))
 ) as c40022,
collect_set(if(month_log_u_spwd_check_vcode is null or month_log_u_spwd_check_vcode = 0, null,
 concat_ws('=', concat_ws('_', month, '40023'), cast(month_log_u_spwd_check_vcode as string)))
 ) as c40023,
collect_set(if(month_log_u_spwd_verify_vcode is null or month_log_u_spwd_verify_vcode = 0, null,
 concat_ws('=', concat_ws('_', month, '40024'), cast(month_log_u_spwd_verify_vcode as string)))
 ) as c40024,
collect_set(if(month_log_u_spwd_get_vcode is null or month_log_u_spwd_get_vcode = 0, null,
 concat_ws('=', concat_ws('_', month, '40025'), cast(month_log_u_spwd_get_vcode as string)))
 ) as c40025,
collect_set(if(month_log_u_spwd_check_spwd is null or month_log_u_spwd_check_spwd = 0, null,
 concat_ws('=', concat_ws('_', month, '40026'), cast(month_log_u_spwd_check_spwd as string)))
 ) as c40026,
collect_set(if(month_log_u_spwd_reset_pwd is null or month_log_u_spwd_reset_pwd = 0, null,
 concat_ws('=', concat_ws('_', month, '40027'), cast(month_log_u_spwd_reset_pwd as string)))
 ) as c40027,
collect_set(if(month_log_u_reset_pwd_check_code is null or month_log_u_reset_pwd_check_code = 0, null,
 concat_ws('=', concat_ws('_', month, '40028'), cast(month_log_u_reset_pwd_check_code as string)))
 ) as c40028,
collect_set(if(month_log_u_find_pwd is null or month_log_u_find_pwd = 0, null,
 concat_ws('=', concat_ws('_', month, '40029'), cast(month_log_u_find_pwd as string)))
 ) as c40029,
collect_set(if(month_log_u_spwd_get_pk is null or month_log_u_spwd_get_pk = 0, null,
 concat_ws('=', concat_ws('_', month, '40030'), cast(month_log_u_spwd_get_pk as string)))
 ) as c40030,
collect_set(if(month_log_u_spwd_add_spwd is null or month_log_u_spwd_add_spwd = 0, null,
 concat_ws('=', concat_ws('_', month, '40031'), cast(month_log_u_spwd_add_spwd as string)))
 ) as c40031,
collect_set(if(month_log_u_login is null or month_log_u_login = 0, null,
 concat_ws('=', concat_ws('_', month, '40032'), cast(month_log_u_login as string)))
 ) as c40032,
collect_set(if(month_log_u_login_2 is null or month_log_u_login_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40033'), cast(month_log_u_login_2 as string)))
 ) as c40033,
collect_set(if(month_log_u_login_3 is null or month_log_u_login_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40034'), cast(month_log_u_login_3 as string)))
 ) as c40034,
collect_set(if(month_log_u_auto_login is null or month_log_u_auto_login = 0, null,
 concat_ws('=', concat_ws('_', month, '40035'), cast(month_log_u_auto_login as string)))
 ) as c40035,
collect_set(if(month_log_u_login_send_code is null or month_log_u_login_send_code = 0, null,
 concat_ws('=', concat_ws('_', month, '40036'), cast(month_log_u_login_send_code as string)))
 ) as c40036,
collect_set(if(month_log_u_spwd_login_by_vcode is null or month_log_u_spwd_login_by_vcode = 0, null,
 concat_ws('=', concat_ws('_', month, '40037'), cast(month_log_u_spwd_login_by_vcode as string)))
 ) as c40037,
collect_set(if(month_log_u_spwd_login_by_mobile is null or month_log_u_spwd_login_by_mobile = 0, null,
 concat_ws('=', concat_ws('_', month, '40038'), cast(month_log_u_spwd_login_by_mobile as string)))
 ) as c40038,
collect_set(if(month_log_u_logout is null or month_log_u_logout = 0, null,
 concat_ws('=', concat_ws('_', month, '40039'), cast(month_log_u_logout as string)))
 ) as c40039,
collect_set(if(month_log_u_update_cookie is null or month_log_u_update_cookie = 0, null,
 concat_ws('=', concat_ws('_', month, '40040'), cast(month_log_u_update_cookie as string)))
 ) as c40040,
collect_set(if(month_log_u_click_ads is null or month_log_u_click_ads = 0, null,
 concat_ws('=', concat_ws('_', month, '40041'), cast(month_log_u_click_ads as string)))
 ) as c40041,
collect_set(if(month_log_u_modify_msg__seq is null or month_log_u_modify_msg__seq = 0, null,
 concat_ws('=', concat_ws('_', month, '40042'), cast(month_log_u_modify_msg__seq as string)))
 ) as c40042,
collect_set(if(month_log_u_make_all_msg_read is null or month_log_u_make_all_msg_read = 0, null,
 concat_ws('=', concat_ws('_', month, '40043'), cast(month_log_u_make_all_msg_read as string)))
 ) as c40043,
collect_set(if(month_log_u_del_msg is null or month_log_u_del_msg = 0, null,
 concat_ws('=', concat_ws('_', month, '40044'), cast(month_log_u_del_msg as string)))
 ) as c40044,
collect_set(if(month_log_u_click_msg is null or month_log_u_click_msg = 0, null,
 concat_ws('=', concat_ws('_', month, '40045'), cast(month_log_u_click_msg as string)))
 ) as c40045,
collect_set(if(month_log_u_read_msg is null or month_log_u_read_msg = 0, null,
 concat_ws('=', concat_ws('_', month, '40046'), cast(month_log_u_read_msg as string)))
 ) as c40046,
collect_set(if(month_log_u_click_msg_box is null or month_log_u_click_msg_box = 0, null,
 concat_ws('=', concat_ws('_', month, '40047'), cast(month_log_u_click_msg_box as string)))
 ) as c40047,
collect_set(if(month_log_u_click_red_point is null or month_log_u_click_red_point = 0, null,
 concat_ws('=', concat_ws('_', month, '40048'), cast(month_log_u_click_red_point as string)))
 ) as c40048,
collect_set(if(month_log_u_clear_red_point is null or month_log_u_clear_red_point = 0, null,
 concat_ws('=', concat_ws('_', month, '40049'), cast(month_log_u_clear_red_point as string)))
 ) as c40049,
collect_set(if(month_log_u_click_black_remind is null or month_log_u_click_black_remind = 0, null,
 concat_ws('=', concat_ws('_', month, '40050'), cast(month_log_u_click_black_remind as string)))
 ) as c40050,
collect_set(if(month_log_u_list_invoice is null or month_log_u_list_invoice = 0, null,
 concat_ws('=', concat_ws('_', month, '40051'), cast(month_log_u_list_invoice as string)))
 ) as c40051,
collect_set(if(month_log_u_list_invoice_2 is null or month_log_u_list_invoice_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40052'), cast(month_log_u_list_invoice_2 as string)))
 ) as c40052,
collect_set(if(month_log_u_list_invoice_head is null or month_log_u_list_invoice_head = 0, null,
 concat_ws('=', concat_ws('_', month, '40053'), cast(month_log_u_list_invoice_head as string)))
 ) as c40053,
collect_set(if(month_log_u_invoice_detail is null or month_log_u_invoice_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40054'), cast(month_log_u_invoice_detail as string)))
 ) as c40054,
collect_set(if(month_log_u_query_logistics_info is null or month_log_u_query_logistics_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40055'), cast(month_log_u_query_logistics_info as string)))
 ) as c40055,
collect_set(if(month_log_u_list_address is null or month_log_u_list_address = 0, null,
 concat_ws('=', concat_ws('_', month, '40056'), cast(month_log_u_list_address as string)))
 ) as c40056,
collect_set(if(month_log_u_del_bankcard is null or month_log_u_del_bankcard = 0, null,
 concat_ws('=', concat_ws('_', month, '40057'), cast(month_log_u_del_bankcard as string)))
 ) as c40057,
collect_set(if(month_log_u_list_bankcard is null or month_log_u_list_bankcard = 0, null,
 concat_ws('=', concat_ws('_', month, '40058'), cast(month_log_u_list_bankcard as string)))
 ) as c40058,
collect_set(if(month_log_u_journey_tab is null or month_log_u_journey_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40059'), cast(month_log_u_journey_tab as string)))
 ) as c40059,
collect_set(if(month_log_u_holiday_tab is null or month_log_u_holiday_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40060'), cast(month_log_u_holiday_tab as string)))
 ) as c40060,
collect_set(if(month_log_u_ticket_tab is null or month_log_u_ticket_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40061'), cast(month_log_u_ticket_tab as string)))
 ) as c40061,
collect_set(if(month_log_u_taxi_tab is null or month_log_u_taxi_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40062'), cast(month_log_u_taxi_tab as string)))
 ) as c40062,
collect_set(if(month_log_u_discover_tab is null or month_log_u_discover_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40063'), cast(month_log_u_discover_tab as string)))
 ) as c40063,
collect_set(if(month_log_u_discover_tab_2 is null or month_log_u_discover_tab_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40064'), cast(month_log_u_discover_tab_2 as string)))
 ) as c40064,
collect_set(if(month_log_u_home_tab is null or month_log_u_home_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40065'), cast(month_log_u_home_tab as string)))
 ) as c40065,
collect_set(if(month_log_u_list_devices is null or month_log_u_list_devices = 0, null,
 concat_ws('=', concat_ws('_', month, '40066'), cast(month_log_u_list_devices as string)))
 ) as c40066,
collect_set(if(month_log_u_pay_order is null or month_log_u_pay_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40067'), cast(month_log_u_pay_order as string)))
 ) as c40067,
collect_set(if(month_log_u_link_order is null or month_log_u_link_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40068'), cast(month_log_u_link_order as string)))
 ) as c40068,
collect_set(if(month_log_u_recommend_with_logout is null or month_log_u_recommend_with_logout = 0, null,
 concat_ws('=', concat_ws('_', month, '40069'), cast(month_log_u_recommend_with_logout as string)))
 ) as c40069,
collect_set(if(month_log_u_get_user_by_cookie is null or month_log_u_get_user_by_cookie = 0, null,
 concat_ws('=', concat_ws('_', month, '40070'), cast(month_log_u_get_user_by_cookie as string)))
 ) as c40070,
collect_set(if(month_log_u_check_student is null or month_log_u_check_student = 0, null,
 concat_ws('=', concat_ws('_', month, '40071'), cast(month_log_u_check_student as string)))
 ) as c40071,
collect_set(if(month_log_u_advice is null or month_log_u_advice = 0, null,
 concat_ws('=', concat_ws('_', month, '40072'), cast(month_log_u_advice as string)))
 ) as c40072,
collect_set(if(month_log_u_click_useful is null or month_log_u_click_useful = 0, null,
 concat_ws('=', concat_ws('_', month, '40073'), cast(month_log_u_click_useful as string)))
 ) as c40073,
collect_set(if(month_log_u_list_coupon is null or month_log_u_list_coupon = 0, null,
 concat_ws('=', concat_ws('_', month, '40074'), cast(month_log_u_list_coupon as string)))
 ) as c40074,
collect_set(if(month_log_u_list_balance is null or month_log_u_list_balance = 0, null,
 concat_ws('=', concat_ws('_', month, '40075'), cast(month_log_u_list_balance as string)))
 ) as c40075,
collect_set(if(month_log_u_list_property is null or month_log_u_list_property = 0, null,
 concat_ws('=', concat_ws('_', month, '40076'), cast(month_log_u_list_property as string)))
 ) as c40076,
collect_set(if(month_log_u_invite_friends is null or month_log_u_invite_friends = 0, null,
 concat_ws('=', concat_ws('_', month, '40077'), cast(month_log_u_invite_friends as string)))
 ) as c40077,
collect_set(if(month_log_u_invite_confirm is null or month_log_u_invite_confirm = 0, null,
 concat_ws('=', concat_ws('_', month, '40078'), cast(month_log_u_invite_confirm as string)))
 ) as c40078,
collect_set(if(month_log_u_list_product is null or month_log_u_list_product = 0, null,
 concat_ws('=', concat_ws('_', month, '40079'), cast(month_log_u_list_product as string)))
 ) as c40079,
collect_set(if(month_log_u_ota_docs is null or month_log_u_ota_docs = 0, null,
 concat_ws('=', concat_ws('_', month, '40080'), cast(month_log_u_ota_docs as string)))
 ) as c40080

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_user_month
WHERE is_not_null(user_id)
) t9
ON t.user_id = t9.user_id

WHERE
    t9.month >= t.begin_month and t9.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 4.2: 日志图-机票

CREATE VIEW tsfm.log_flight_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_f_init is null or month_log_f_init = 0, null,
 concat_ws('=', concat_ws('_', month, '40081'), cast(month_log_f_init as string)))
 ) as c40081,
collect_set(if(month_log_f_tab is null or month_log_f_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40082'), cast(month_log_f_tab as string)))
 ) as c40082,
collect_set(if(month_log_f_bargain_tab is null or month_log_f_bargain_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40083'), cast(month_log_f_bargain_tab as string)))
 ) as c40083,
collect_set(if(month_log_f_bargain_tab_2 is null or month_log_f_bargain_tab_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40084'), cast(month_log_f_bargain_tab_2 as string)))
 ) as c40084,
collect_set(if(month_log_f_home_notice is null or month_log_f_home_notice = 0, null,
 concat_ws('=', concat_ws('_', month, '40085'), cast(month_log_f_home_notice as string)))
 ) as c40085,
collect_set(if(month_log_f_home_banner is null or month_log_f_home_banner = 0, null,
 concat_ws('=', concat_ws('_', month, '40086'), cast(month_log_f_home_banner as string)))
 ) as c40086,
collect_set(if(month_log_f_select_date is null or month_log_f_select_date = 0, null,
 concat_ws('=', concat_ws('_', month, '40087'), cast(month_log_f_select_date as string)))
 ) as c40087,
collect_set(if(month_log_f_search_one_way is null or month_log_f_search_one_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40088'), cast(month_log_f_search_one_way as string)))
 ) as c40088,
collect_set(if(month_log_f_bargain_round_way is null or month_log_f_bargain_round_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40089'), cast(month_log_f_bargain_round_way as string)))
 ) as c40089,
collect_set(if(month_log_f_search_round_way is null or month_log_f_search_round_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40090'), cast(month_log_f_search_round_way as string)))
 ) as c40090,
collect_set(if(month_log_f_search_round_way_2 is null or month_log_f_search_round_way_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40091'), cast(month_log_f_search_round_way_2 as string)))
 ) as c40091,
collect_set(if(month_log_f_search_multi_way is null or month_log_f_search_multi_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40092'), cast(month_log_f_search_multi_way as string)))
 ) as c40092,
collect_set(if(month_log_f_search_low_price is null or month_log_f_search_low_price = 0, null,
 concat_ws('=', concat_ws('_', month, '40093'), cast(month_log_f_search_low_price as string)))
 ) as c40093,
collect_set(if(month_log_f_search_record is null or month_log_f_search_record = 0, null,
 concat_ws('=', concat_ws('_', month, '40094'), cast(month_log_f_search_record as string)))
 ) as c40094,
collect_set(if(month_log_f_list_flight is null or month_log_f_list_flight = 0, null,
 concat_ws('=', concat_ws('_', month, '40095'), cast(month_log_f_list_flight as string)))
 ) as c40095,
collect_set(if(month_log_f_package is null or month_log_f_package = 0, null,
 concat_ws('=', concat_ws('_', month, '40096'), cast(month_log_f_package as string)))
 ) as c40096,
collect_set(if(month_log_f_list_inter_multi_way is null or month_log_f_list_inter_multi_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40097'), cast(month_log_f_list_inter_multi_way as string)))
 ) as c40097,
collect_set(if(month_log_f_search_trend is null or month_log_f_search_trend = 0, null,
 concat_ws('=', concat_ws('_', month, '40098'), cast(month_log_f_search_trend as string)))
 ) as c40098,
collect_set(if(month_log_f_search_trend_2 is null or month_log_f_search_trend_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40099'), cast(month_log_f_search_trend_2 as string)))
 ) as c40099,
collect_set(if(month_log_f_price_table_round_way is null or month_log_f_price_table_round_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40100'), cast(month_log_f_price_table_round_way as string)))
 ) as c40100,
collect_set(if(month_log_f_his_search_collect is null or month_log_f_his_search_collect = 0, null,
 concat_ws('=', concat_ws('_', month, '40101'), cast(month_log_f_his_search_collect as string)))
 ) as c40101,
collect_set(if(month_log_f_inter_cheapest is null or month_log_f_inter_cheapest = 0, null,
 concat_ws('=', concat_ws('_', month, '40102'), cast(month_log_f_inter_cheapest as string)))
 ) as c40102,
collect_set(if(month_log_f_search_bargain is null or month_log_f_search_bargain = 0, null,
 concat_ws('=', concat_ws('_', month, '40103'), cast(month_log_f_search_bargain as string)))
 ) as c40103,
collect_set(if(month_log_f_search_inter_bargain is null or month_log_f_search_inter_bargain = 0, null,
 concat_ws('=', concat_ws('_', month, '40104'), cast(month_log_f_search_inter_bargain as string)))
 ) as c40104,
collect_set(if(month_log_f_search_inter_bargain2 is null or month_log_f_search_inter_bargain2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40105'), cast(month_log_f_search_inter_bargain2 as string)))
 ) as c40105,
collect_set(if(month_log_f_search_car_init_price is null or month_log_f_search_car_init_price = 0, null,
 concat_ws('=', concat_ws('_', month, '40106'), cast(month_log_f_search_car_init_price as string)))
 ) as c40106,
collect_set(if(month_log_f_fuzzy_list_country is null or month_log_f_fuzzy_list_country = 0, null,
 concat_ws('=', concat_ws('_', month, '40107'), cast(month_log_f_fuzzy_list_country as string)))
 ) as c40107,
collect_set(if(month_log_f_list_inter_country is null or month_log_f_list_inter_country = 0, null,
 concat_ws('=', concat_ws('_', month, '40108'), cast(month_log_f_list_inter_country as string)))
 ) as c40108,
collect_set(if(month_log_f_nearby_recommend is null or month_log_f_nearby_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40109'), cast(month_log_f_nearby_recommend as string)))
 ) as c40109,
collect_set(if(month_log_f_sort_one_way is null or month_log_f_sort_one_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40110'), cast(month_log_f_sort_one_way as string)))
 ) as c40110,
collect_set(if(month_log_f_sort_round_way is null or month_log_f_sort_round_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40111'), cast(month_log_f_sort_round_way as string)))
 ) as c40111,
collect_set(if(month_log_f_sift_one_way is null or month_log_f_sift_one_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40112'), cast(month_log_f_sift_one_way as string)))
 ) as c40112,
collect_set(if(month_log_f_sift_round_way is null or month_log_f_sift_round_way = 0, null,
 concat_ws('=', concat_ws('_', month, '40113'), cast(month_log_f_sift_round_way as string)))
 ) as c40113,
collect_set(if(month_log_f_detail is null or month_log_f_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40114'), cast(month_log_f_detail as string)))
 ) as c40114,
collect_set(if(month_log_f_detail_2 is null or month_log_f_detail_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40115'), cast(month_log_f_detail_2 as string)))
 ) as c40115,
collect_set(if(month_log_f_one_way_detail is null or month_log_f_one_way_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40116'), cast(month_log_f_one_way_detail as string)))
 ) as c40116,
collect_set(if(month_log_f_inter_one_way_detail is null or month_log_f_inter_one_way_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40117'), cast(month_log_f_inter_one_way_detail as string)))
 ) as c40117,
collect_set(if(month_log_f_round_way_detail is null or month_log_f_round_way_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40118'), cast(month_log_f_round_way_detail as string)))
 ) as c40118,
collect_set(if(month_log_f_inter_round_detail is null or month_log_f_inter_round_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40119'), cast(month_log_f_inter_round_detail as string)))
 ) as c40119,
collect_set(if(month_log_f_more_way_detail is null or month_log_f_more_way_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40120'), cast(month_log_f_more_way_detail as string)))
 ) as c40120,
collect_set(if(month_log_f_subscribe is null or month_log_f_subscribe = 0, null,
 concat_ws('=', concat_ws('_', month, '40121'), cast(month_log_f_subscribe as string)))
 ) as c40121,
collect_set(if(month_log_f_subscribe_2 is null or month_log_f_subscribe_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40122'), cast(month_log_f_subscribe_2 as string)))
 ) as c40122,
collect_set(if(month_log_f_subscribe_3 is null or month_log_f_subscribe_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40123'), cast(month_log_f_subscribe_3 as string)))
 ) as c40123,
collect_set(if(month_log_f_add_subscribe is null or month_log_f_add_subscribe = 0, null,
 concat_ws('=', concat_ws('_', month, '40124'), cast(month_log_f_add_subscribe as string)))
 ) as c40124,
collect_set(if(month_log_f_add_subscribe_2 is null or month_log_f_add_subscribe_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40125'), cast(month_log_f_add_subscribe_2 as string)))
 ) as c40125,
collect_set(if(month_log_f_edit_subscribe is null or month_log_f_edit_subscribe = 0, null,
 concat_ws('=', concat_ws('_', month, '40126'), cast(month_log_f_edit_subscribe as string)))
 ) as c40126,
collect_set(if(month_log_f_edit_subscribe_2 is null or month_log_f_edit_subscribe_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40127'), cast(month_log_f_edit_subscribe_2 as string)))
 ) as c40127,
collect_set(if(month_log_f_get_subscribe_rate is null or month_log_f_get_subscribe_rate = 0, null,
 concat_ws('=', concat_ws('_', month, '40128'), cast(month_log_f_get_subscribe_rate as string)))
 ) as c40128,
collect_set(if(month_log_f_subscribe_detail is null or month_log_f_subscribe_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40129'), cast(month_log_f_subscribe_detail as string)))
 ) as c40129,
collect_set(if(month_log_f_subscribe_count is null or month_log_f_subscribe_count = 0, null,
 concat_ws('=', concat_ws('_', month, '40130'), cast(month_log_f_subscribe_count as string)))
 ) as c40130,
collect_set(if(month_log_f_del_subscribe is null or month_log_f_del_subscribe = 0, null,
 concat_ws('=', concat_ws('_', month, '40131'), cast(month_log_f_del_subscribe as string)))
 ) as c40131,
collect_set(if(month_log_f_subscribe_check_phone is null or month_log_f_subscribe_check_phone = 0, null,
 concat_ws('=', concat_ws('_', month, '40132'), cast(month_log_f_subscribe_check_phone as string)))
 ) as c40132,
collect_set(if(month_log_f_subscribe_recommend is null or month_log_f_subscribe_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40133'), cast(month_log_f_subscribe_recommend as string)))
 ) as c40133,
collect_set(if(month_log_f_update_passenger is null or month_log_f_update_passenger = 0, null,
 concat_ws('=', concat_ws('_', month, '40134'), cast(month_log_f_update_passenger as string)))
 ) as c40134,
collect_set(if(month_log_f_validate_passenger is null or month_log_f_validate_passenger = 0, null,
 concat_ws('=', concat_ws('_', month, '40135'), cast(month_log_f_validate_passenger as string)))
 ) as c40135,
collect_set(if(month_log_f_add_passenger is null or month_log_f_add_passenger = 0, null,
 concat_ws('=', concat_ws('_', month, '40136'), cast(month_log_f_add_passenger as string)))
 ) as c40136,
collect_set(if(month_log_f_list_passenger is null or month_log_f_list_passenger = 0, null,
 concat_ws('=', concat_ws('_', month, '40137'), cast(month_log_f_list_passenger as string)))
 ) as c40137,
collect_set(if(month_log_f_check_phone is null or month_log_f_check_phone = 0, null,
 concat_ws('=', concat_ws('_', month, '40138'), cast(month_log_f_check_phone as string)))
 ) as c40138,
collect_set(if(month_log_f_av_check is null or month_log_f_av_check = 0, null,
 concat_ws('=', concat_ws('_', month, '40139'), cast(month_log_f_av_check as string)))
 ) as c40139,
collect_set(if(month_log_f_av_check_2 is null or month_log_f_av_check_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40140'), cast(month_log_f_av_check_2 as string)))
 ) as c40140,
collect_set(if(month_log_f_inter_av_check is null or month_log_f_inter_av_check = 0, null,
 concat_ws('=', concat_ws('_', month, '40141'), cast(month_log_f_inter_av_check as string)))
 ) as c40141,
collect_set(if(month_log_f_inter_av_check_2 is null or month_log_f_inter_av_check_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40142'), cast(month_log_f_inter_av_check_2 as string)))
 ) as c40142,
collect_set(if(month_log_f_inter_av_check_3 is null or month_log_f_inter_av_check_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40143'), cast(month_log_f_inter_av_check_3 as string)))
 ) as c40143,
collect_set(if(month_log_f_av_check_3 is null or month_log_f_av_check_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40144'), cast(month_log_f_av_check_3 as string)))
 ) as c40144,
collect_set(if(month_log_f_check_prepay is null or month_log_f_check_prepay = 0, null,
 concat_ws('=', concat_ws('_', month, '40145'), cast(month_log_f_check_prepay as string)))
 ) as c40145,
collect_set(if(month_log_f_check_prepay_2 is null or month_log_f_check_prepay_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40146'), cast(month_log_f_check_prepay_2 as string)))
 ) as c40146,
collect_set(if(month_log_f_submit_at_once is null or month_log_f_submit_at_once = 0, null,
 concat_ws('=', concat_ws('_', month, '40147'), cast(month_log_f_submit_at_once as string)))
 ) as c40147,
collect_set(if(month_log_f_submit is null or month_log_f_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40148'), cast(month_log_f_submit as string)))
 ) as c40148,
collect_set(if(month_log_f_submit_2 is null or month_log_f_submit_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40149'), cast(month_log_f_submit_2 as string)))
 ) as c40149,
collect_set(if(month_log_f_merged_submit is null or month_log_f_merged_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40150'), cast(month_log_f_merged_submit as string)))
 ) as c40150,
collect_set(if(month_log_f_inter_submit is null or month_log_f_inter_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40151'), cast(month_log_f_inter_submit as string)))
 ) as c40151,
collect_set(if(month_log_f_inner_merge_submit is null or month_log_f_inner_merge_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40152'), cast(month_log_f_inner_merge_submit as string)))
 ) as c40152,
collect_set(if(month_log_f_inner_merge_submit_2 is null or month_log_f_inner_merge_submit_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40153'), cast(month_log_f_inner_merge_submit_2 as string)))
 ) as c40153,
collect_set(if(month_log_f_inter_merge_submit is null or month_log_f_inter_merge_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40154'), cast(month_log_f_inter_merge_submit as string)))
 ) as c40154,
collect_set(if(month_log_f_pay_success is null or month_log_f_pay_success = 0, null,
 concat_ws('=', concat_ws('_', month, '40155'), cast(month_log_f_pay_success as string)))
 ) as c40155,
collect_set(if(month_log_f_check_after_pay is null or month_log_f_check_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40156'), cast(month_log_f_check_after_pay as string)))
 ) as c40156,
collect_set(if(month_log_f_recommend_after_pay is null or month_log_f_recommend_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40157'), cast(month_log_f_recommend_after_pay as string)))
 ) as c40157,
collect_set(if(month_log_f_share_after_pay is null or month_log_f_share_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40158'), cast(month_log_f_share_after_pay as string)))
 ) as c40158,
collect_set(if(month_log_f_get_package_after_pay is null or month_log_f_get_package_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40159'), cast(month_log_f_get_package_after_pay as string)))
 ) as c40159,
collect_set(if(month_log_f_list_order is null or month_log_f_list_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40160'), cast(month_log_f_list_order as string)))
 ) as c40160,
collect_set(if(month_log_f_list_order_2 is null or month_log_f_list_order_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40161'), cast(month_log_f_list_order_2 as string)))
 ) as c40161,
collect_set(if(month_log_f_order_detail is null or month_log_f_order_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40162'), cast(month_log_f_order_detail as string)))
 ) as c40162,
collect_set(if(month_log_f_order_detail_2 is null or month_log_f_order_detail_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40163'), cast(month_log_f_order_detail_2 as string)))
 ) as c40163,
collect_set(if(month_log_f_list_order_by_vcode is null or month_log_f_list_order_by_vcode = 0, null,
 concat_ws('=', concat_ws('_', month, '40164'), cast(month_log_f_list_order_by_vcode as string)))
 ) as c40164,
collect_set(if(month_log_f_follow_status is null or month_log_f_follow_status = 0, null,
 concat_ws('=', concat_ws('_', month, '40165'), cast(month_log_f_follow_status as string)))
 ) as c40165,
collect_set(if(month_log_f_follow_status_2 is null or month_log_f_follow_status_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40166'), cast(month_log_f_follow_status_2 as string)))
 ) as c40166,
collect_set(if(month_log_f_list_sms_user is null or month_log_f_list_sms_user = 0, null,
 concat_ws('=', concat_ws('_', month, '40167'), cast(month_log_f_list_sms_user as string)))
 ) as c40167,
collect_set(if(month_log_f_get_status is null or month_log_f_get_status = 0, null,
 concat_ws('=', concat_ws('_', month, '40168'), cast(month_log_f_get_status as string)))
 ) as c40168,
collect_set(if(month_log_f_list_status is null or month_log_f_list_status = 0, null,
 concat_ws('=', concat_ws('_', month, '40169'), cast(month_log_f_list_status as string)))
 ) as c40169,
collect_set(if(month_log_f_status_detail is null or month_log_f_status_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40170'), cast(month_log_f_status_detail as string)))
 ) as c40170,
collect_set(if(month_log_f_get_ota_info is null or month_log_f_get_ota_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40171'), cast(month_log_f_get_ota_info as string)))
 ) as c40171,
collect_set(if(month_log_f_get_ota_info_2 is null or month_log_f_get_ota_info_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40172'), cast(month_log_f_get_ota_info_2 as string)))
 ) as c40172,
collect_set(if(month_log_f_send_vcode4order is null or month_log_f_send_vcode4order = 0, null,
 concat_ws('=', concat_ws('_', month, '40173'), cast(month_log_f_send_vcode4order as string)))
 ) as c40173,
collect_set(if(month_log_f_order2wap is null or month_log_f_order2wap = 0, null,
 concat_ws('=', concat_ws('_', month, '40174'), cast(month_log_f_order2wap as string)))
 ) as c40174,
collect_set(if(month_log_f_urge_ticket is null or month_log_f_urge_ticket = 0, null,
 concat_ws('=', concat_ws('_', month, '40175'), cast(month_log_f_urge_ticket as string)))
 ) as c40175,
collect_set(if(month_log_f_list_question is null or month_log_f_list_question = 0, null,
 concat_ws('=', concat_ws('_', month, '40176'), cast(month_log_f_list_question as string)))
 ) as c40176,
collect_set(if(month_log_f_list_question_detail is null or month_log_f_list_question_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40177'), cast(month_log_f_list_question_detail as string)))
 ) as c40177,
collect_set(if(month_log_f_get_detail4check_in is null or month_log_f_get_detail4check_in = 0, null,
 concat_ws('=', concat_ws('_', month, '40178'), cast(month_log_f_get_detail4check_in as string)))
 ) as c40178,
collect_set(if(month_log_f_log4check_in is null or month_log_f_log4check_in = 0, null,
 concat_ws('=', concat_ws('_', month, '40179'), cast(month_log_f_log4check_in as string)))
 ) as c40179,
collect_set(if(month_log_f_lua4check_in is null or month_log_f_lua4check_in = 0, null,
 concat_ws('=', concat_ws('_', month, '40180'), cast(month_log_f_lua4check_in as string)))
 ) as c40180,
collect_set(if(month_log_f_check_in is null or month_log_f_check_in = 0, null,
 concat_ws('=', concat_ws('_', month, '40181'), cast(month_log_f_check_in as string)))
 ) as c40181,
collect_set(if(month_log_f_del_order is null or month_log_f_del_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40182'), cast(month_log_f_del_order as string)))
 ) as c40182,
collect_set(if(month_log_f_dbt is null or month_log_f_dbt = 0, null,
 concat_ws('=', concat_ws('_', month, '40183'), cast(month_log_f_dbt as string)))
 ) as c40183,
collect_set(if(month_log_f_suggest_city is null or month_log_f_suggest_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40184'), cast(month_log_f_suggest_city as string)))
 ) as c40184,
collect_set(if(month_log_f_biz_recommend is null or month_log_f_biz_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40185'), cast(month_log_f_biz_recommend as string)))
 ) as c40185,
collect_set(if(month_log_f_trend_banner is null or month_log_f_trend_banner = 0, null,
 concat_ws('=', concat_ws('_', month, '40186'), cast(month_log_f_trend_banner as string)))
 ) as c40186,
collect_set(if(month_log_f_update_city is null or month_log_f_update_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40187'), cast(month_log_f_update_city as string)))
 ) as c40187,
collect_set(if(month_log_f_recommend_by_car is null or month_log_f_recommend_by_car = 0, null,
 concat_ws('=', concat_ws('_', month, '40188'), cast(month_log_f_recommend_by_car as string)))
 ) as c40188,
collect_set(if(month_log_f_low_price4train is null or month_log_f_low_price4train = 0, null,
 concat_ws('=', concat_ws('_', month, '40189'), cast(month_log_f_low_price4train as string)))
 ) as c40189,
collect_set(if(month_log_f_c2b_stop_time is null or month_log_f_c2b_stop_time = 0, null,
 concat_ws('=', concat_ws('_', month, '40190'), cast(month_log_f_c2b_stop_time as string)))
 ) as c40190

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_flight_month
WHERE is_not_null(user_id)
) t10
ON t.user_id = t10.user_id

WHERE
    t10.month >= t.begin_month and t10.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 4.3: 日志图-火车票+汽车票

CREATE VIEW tsfm.log_train_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_t_access_dispatch is null or month_log_t_access_dispatch = 0, null,
 concat_ws('=', concat_ws('_', month, '40191'), cast(month_log_t_access_dispatch as string)))
 ) as c40191,
collect_set(if(month_log_t_home_tab is null or month_log_t_home_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40192'), cast(month_log_t_home_tab as string)))
 ) as c40192,
collect_set(if(month_log_t_biz_switch is null or month_log_t_biz_switch = 0, null,
 concat_ws('=', concat_ws('_', month, '40193'), cast(month_log_t_biz_switch as string)))
 ) as c40193,
collect_set(if(month_log_t_home_tab_2 is null or month_log_t_home_tab_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40194'), cast(month_log_t_home_tab_2 as string)))
 ) as c40194,
collect_set(if(month_log_t_inter_tab is null or month_log_t_inter_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40195'), cast(month_log_t_inter_tab as string)))
 ) as c40195,
collect_set(if(month_log_t_home_banner is null or month_log_t_home_banner = 0, null,
 concat_ws('=', concat_ws('_', month, '40196'), cast(month_log_t_home_banner as string)))
 ) as c40196,
collect_set(if(month_log_t_student_tab is null or month_log_t_student_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40197'), cast(month_log_t_student_tab as string)))
 ) as c40197,
collect_set(if(month_log_t_red_point is null or month_log_t_red_point = 0, null,
 concat_ws('=', concat_ws('_', month, '40198'), cast(month_log_t_red_point as string)))
 ) as c40198,
collect_set(if(month_log_t_price_table is null or month_log_t_price_table = 0, null,
 concat_ws('=', concat_ws('_', month, '40199'), cast(month_log_t_price_table as string)))
 ) as c40199,
collect_set(if(month_log_t_list_price is null or month_log_t_list_price = 0, null,
 concat_ws('=', concat_ws('_', month, '40200'), cast(month_log_t_list_price as string)))
 ) as c40200,
collect_set(if(month_log_t_search_city2city is null or month_log_t_search_city2city = 0, null,
 concat_ws('=', concat_ws('_', month, '40201'), cast(month_log_t_search_city2city as string)))
 ) as c40201,
collect_set(if(month_log_t_search_city2city_2 is null or month_log_t_search_city2city_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40202'), cast(month_log_t_search_city2city_2 as string)))
 ) as c40202,
collect_set(if(month_log_t_suggest_city is null or month_log_t_suggest_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40203'), cast(month_log_t_suggest_city as string)))
 ) as c40203,
collect_set(if(month_log_t_suggest_city_2 is null or month_log_t_suggest_city_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40204'), cast(month_log_t_suggest_city_2 as string)))
 ) as c40204,
collect_set(if(month_log_t_change_search_city is null or month_log_t_change_search_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40205'), cast(month_log_t_change_search_city as string)))
 ) as c40205,
collect_set(if(month_log_t_change_search_city_2 is null or month_log_t_change_search_city_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40206'), cast(month_log_t_change_search_city_2 as string)))
 ) as c40206,
collect_set(if(month_log_t_search_line is null or month_log_t_search_line = 0, null,
 concat_ws('=', concat_ws('_', month, '40207'), cast(month_log_t_search_line as string)))
 ) as c40207,
collect_set(if(month_log_t_search_line_2 is null or month_log_t_search_line_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40208'), cast(month_log_t_search_line_2 as string)))
 ) as c40208,
collect_set(if(month_log_t_suggest_line is null or month_log_t_suggest_line = 0, null,
 concat_ws('=', concat_ws('_', month, '40209'), cast(month_log_t_suggest_line as string)))
 ) as c40209,
collect_set(if(month_log_t_search_city is null or month_log_t_search_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40210'), cast(month_log_t_search_city as string)))
 ) as c40210,
collect_set(if(month_log_t_order_from_search is null or month_log_t_order_from_search = 0, null,
 concat_ws('=', concat_ws('_', month, '40211'), cast(month_log_t_order_from_search as string)))
 ) as c40211,
collect_set(if(month_log_t_order_booking is null or month_log_t_order_booking = 0, null,
 concat_ws('=', concat_ws('_', month, '40212'), cast(month_log_t_order_booking as string)))
 ) as c40212,
collect_set(if(month_log_t_get_12306 is null or month_log_t_get_12306 = 0, null,
 concat_ws('=', concat_ws('_', month, '40213'), cast(month_log_t_get_12306 as string)))
 ) as c40213,
collect_set(if(month_log_t_12306_msg is null or month_log_t_12306_msg = 0, null,
 concat_ws('=', concat_ws('_', month, '40214'), cast(month_log_t_12306_msg as string)))
 ) as c40214,
collect_set(if(month_log_t_bind_12306 is null or month_log_t_bind_12306 = 0, null,
 concat_ws('=', concat_ws('_', month, '40215'), cast(month_log_t_bind_12306 as string)))
 ) as c40215,
collect_set(if(month_log_t_list_passenger is null or month_log_t_list_passenger = 0, null,
 concat_ws('=', concat_ws('_', month, '40216'), cast(month_log_t_list_passenger as string)))
 ) as c40216,
collect_set(if(month_log_t_student_ext is null or month_log_t_student_ext = 0, null,
 concat_ws('=', concat_ws('_', month, '40217'), cast(month_log_t_student_ext as string)))
 ) as c40217,
collect_set(if(month_log_t_passenger_verify is null or month_log_t_passenger_verify = 0, null,
 concat_ws('=', concat_ws('_', month, '40218'), cast(month_log_t_passenger_verify as string)))
 ) as c40218,
collect_set(if(month_log_t_select_transport_area is null or month_log_t_select_transport_area = 0, null,
 concat_ws('=', concat_ws('_', month, '40219'), cast(month_log_t_select_transport_area as string)))
 ) as c40219,
collect_set(if(month_log_t_transport_ticket is null or month_log_t_transport_ticket = 0, null,
 concat_ws('=', concat_ws('_', month, '40220'), cast(month_log_t_transport_ticket as string)))
 ) as c40220,
collect_set(if(month_log_t_check_reg is null or month_log_t_check_reg = 0, null,
 concat_ws('=', concat_ws('_', month, '40221'), cast(month_log_t_check_reg as string)))
 ) as c40221,
collect_set(if(month_log_t_phone_endecrypt is null or month_log_t_phone_endecrypt = 0, null,
 concat_ws('=', concat_ws('_', month, '40222'), cast(month_log_t_phone_endecrypt as string)))
 ) as c40222,
collect_set(if(month_log_t_order_submit is null or month_log_t_order_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40223'), cast(month_log_t_order_submit as string)))
 ) as c40223,
collect_set(if(month_log_t_coupon is null or month_log_t_coupon = 0, null,
 concat_ws('=', concat_ws('_', month, '40224'), cast(month_log_t_coupon as string)))
 ) as c40224,
collect_set(if(month_log_t_member_coupon is null or month_log_t_member_coupon = 0, null,
 concat_ws('=', concat_ws('_', month, '40225'), cast(month_log_t_member_coupon as string)))
 ) as c40225,
collect_set(if(month_log_t_check_prepay is null or month_log_t_check_prepay = 0, null,
 concat_ws('=', concat_ws('_', month, '40226'), cast(month_log_t_check_prepay as string)))
 ) as c40226,
collect_set(if(month_log_t_check_after_pay is null or month_log_t_check_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40227'), cast(month_log_t_check_after_pay as string)))
 ) as c40227,
collect_set(if(month_log_t_recommend_after_pay is null or month_log_t_recommend_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40228'), cast(month_log_t_recommend_after_pay as string)))
 ) as c40228,
collect_set(if(month_log_t_list_order is null or month_log_t_list_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40229'), cast(month_log_t_list_order as string)))
 ) as c40229,
collect_set(if(month_log_t_list_order_verify is null or month_log_t_list_order_verify = 0, null,
 concat_ws('=', concat_ws('_', month, '40230'), cast(month_log_t_list_order_verify as string)))
 ) as c40230,
collect_set(if(month_log_t_order_detail is null or month_log_t_order_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40231'), cast(month_log_t_order_detail as string)))
 ) as c40231,
collect_set(if(month_log_t_bind_order is null or month_log_t_bind_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40232'), cast(month_log_t_bind_order as string)))
 ) as c40232,
collect_set(if(month_log_t_list_rob_order is null or month_log_t_list_rob_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40233'), cast(month_log_t_list_rob_order as string)))
 ) as c40233,
collect_set(if(month_log_t_deal_order is null or month_log_t_deal_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40234'), cast(month_log_t_deal_order as string)))
 ) as c40234,
collect_set(if(month_log_t_refund is null or month_log_t_refund = 0, null,
 concat_ws('=', concat_ws('_', month, '40235'), cast(month_log_t_refund as string)))
 ) as c40235,
collect_set(if(month_log_t_refund_2 is null or month_log_t_refund_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40236'), cast(month_log_t_refund_2 as string)))
 ) as c40236,
collect_set(if(month_log_t_abtest is null or month_log_t_abtest = 0, null,
 concat_ws('=', concat_ws('_', month, '40237'), cast(month_log_t_abtest as string)))
 ) as c40237,
collect_set(if(month_log_t_faqs is null or month_log_t_faqs = 0, null,
 concat_ws('=', concat_ws('_', month, '40238'), cast(month_log_t_faqs as string)))
 ) as c40238,
collect_set(if(month_log_c_taxi_tab is null or month_log_c_taxi_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40239'), cast(month_log_c_taxi_tab as string)))
 ) as c40239,
collect_set(if(month_log_c_inter_city is null or month_log_c_inter_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40240'), cast(month_log_c_inter_city as string)))
 ) as c40240,
collect_set(if(month_log_c_tab_update is null or month_log_c_tab_update = 0, null,
 concat_ws('=', concat_ws('_', month, '40241'), cast(month_log_c_tab_update as string)))
 ) as c40241,
collect_set(if(month_log_c_banner is null or month_log_c_banner = 0, null,
 concat_ws('=', concat_ws('_', month, '40242'), cast(month_log_c_banner as string)))
 ) as c40242,
collect_set(if(month_log_c_home_abtest is null or month_log_c_home_abtest = 0, null,
 concat_ws('=', concat_ws('_', month, '40243'), cast(month_log_c_home_abtest as string)))
 ) as c40243,
collect_set(if(month_log_c_default_line is null or month_log_c_default_line = 0, null,
 concat_ws('=', concat_ws('_', month, '40244'), cast(month_log_c_default_line as string)))
 ) as c40244,
collect_set(if(month_log_c_list_city is null or month_log_c_list_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40245'), cast(month_log_c_list_city as string)))
 ) as c40245,
collect_set(if(month_log_c_search_city2city is null or month_log_c_search_city2city = 0, null,
 concat_ws('=', concat_ws('_', month, '40246'), cast(month_log_c_search_city2city as string)))
 ) as c40246,
collect_set(if(month_log_c_search_city2city_2 is null or month_log_c_search_city2city_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40247'), cast(month_log_c_search_city2city_2 as string)))
 ) as c40247,
collect_set(if(month_log_c_search_city2city_3 is null or month_log_c_search_city2city_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40248'), cast(month_log_c_search_city2city_3 as string)))
 ) as c40248,
collect_set(if(month_log_c_line_detail is null or month_log_c_line_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40249'), cast(month_log_c_line_detail as string)))
 ) as c40249,
collect_set(if(month_log_c_city_suggest is null or month_log_c_city_suggest = 0, null,
 concat_ws('=', concat_ws('_', month, '40250'), cast(month_log_c_city_suggest as string)))
 ) as c40250,
collect_set(if(month_log_c_recommend is null or month_log_c_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40251'), cast(month_log_c_recommend as string)))
 ) as c40251,
collect_set(if(month_log_c_recommend_train is null or month_log_c_recommend_train = 0, null,
 concat_ws('=', concat_ws('_', month, '40252'), cast(month_log_c_recommend_train as string)))
 ) as c40252,
collect_set(if(month_log_c_list_passenger is null or month_log_c_list_passenger = 0, null,
 concat_ws('=', concat_ws('_', month, '40253'), cast(month_log_c_list_passenger as string)))
 ) as c40253,
collect_set(if(month_log_c_order_submit is null or month_log_c_order_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40254'), cast(month_log_c_order_submit as string)))
 ) as c40254,
collect_set(if(month_log_c_order_submit_2 is null or month_log_c_order_submit_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40255'), cast(month_log_c_order_submit_2 as string)))
 ) as c40255,
collect_set(if(month_log_c_check_prepay is null or month_log_c_check_prepay = 0, null,
 concat_ws('=', concat_ws('_', month, '40256'), cast(month_log_c_check_prepay as string)))
 ) as c40256,
collect_set(if(month_log_c_list_order is null or month_log_c_list_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40257'), cast(month_log_c_list_order as string)))
 ) as c40257,
collect_set(if(month_log_c_order_detail is null or month_log_c_order_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40258'), cast(month_log_c_order_detail as string)))
 ) as c40258,
collect_set(if(month_log_c_faqs is null or month_log_c_faqs = 0, null,
 concat_ws('=', concat_ws('_', month, '40259'), cast(month_log_c_faqs as string)))
 ) as c40259

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_train_month
WHERE is_not_null(user_id)
) t11
ON t.user_id = t11.user_id

WHERE
    t11.month >= t.begin_month and t11.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 4.4: 日志图-酒店

CREATE VIEW tsfm.log_hotel_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_h_tab is null or month_log_h_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40260'), cast(month_log_h_tab as string)))
 ) as c40260,
collect_set(if(month_log_h_inter_tab is null or month_log_h_inter_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40261'), cast(month_log_h_inter_tab as string)))
 ) as c40261,
collect_set(if(month_log_h_inter_tab_2 is null or month_log_h_inter_tab_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40262'), cast(month_log_h_inter_tab_2 as string)))
 ) as c40262,
collect_set(if(month_log_h_lastmin_tab is null or month_log_h_lastmin_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40263'), cast(month_log_h_lastmin_tab as string)))
 ) as c40263,
collect_set(if(month_log_h_lastmin_tab_2 is null or month_log_h_lastmin_tab_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40264'), cast(month_log_h_lastmin_tab_2 as string)))
 ) as c40264,
collect_set(if(month_log_h_apt_tab is null or month_log_h_apt_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40265'), cast(month_log_h_apt_tab as string)))
 ) as c40265,
collect_set(if(month_log_h_bnb_tab is null or month_log_h_bnb_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40266'), cast(month_log_h_bnb_tab as string)))
 ) as c40266,
collect_set(if(month_log_h_meeting_tab is null or month_log_h_meeting_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40267'), cast(month_log_h_meeting_tab as string)))
 ) as c40267,
collect_set(if(month_log_h_home_bubble is null or month_log_h_home_bubble = 0, null,
 concat_ws('=', concat_ws('_', month, '40268'), cast(month_log_h_home_bubble as string)))
 ) as c40268,
collect_set(if(month_log_h_loading_msg is null or month_log_h_loading_msg = 0, null,
 concat_ws('=', concat_ws('_', month, '40269'), cast(month_log_h_loading_msg as string)))
 ) as c40269,
collect_set(if(month_log_h_init is null or month_log_h_init = 0, null,
 concat_ws('=', concat_ws('_', month, '40270'), cast(month_log_h_init as string)))
 ) as c40270,
collect_set(if(month_log_h_theme_home is null or month_log_h_theme_home = 0, null,
 concat_ws('=', concat_ws('_', month, '40271'), cast(month_log_h_theme_home as string)))
 ) as c40271,
collect_set(if(month_log_h_list_my_hotel is null or month_log_h_list_my_hotel = 0, null,
 concat_ws('=', concat_ws('_', month, '40272'), cast(month_log_h_list_my_hotel as string)))
 ) as c40272,
collect_set(if(month_log_h_list_my_hotel_2 is null or month_log_h_list_my_hotel_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40273'), cast(month_log_h_list_my_hotel_2 as string)))
 ) as c40273,
collect_set(if(month_log_h_collect is null or month_log_h_collect = 0, null,
 concat_ws('=', concat_ws('_', month, '40274'), cast(month_log_h_collect as string)))
 ) as c40274,
collect_set(if(month_log_h_collect_2 is null or month_log_h_collect_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40275'), cast(month_log_h_collect_2 as string)))
 ) as c40275,
collect_set(if(month_log_h_collect_3 is null or month_log_h_collect_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40276'), cast(month_log_h_collect_3 as string)))
 ) as c40276,
collect_set(if(month_log_h_del_collect is null or month_log_h_del_collect = 0, null,
 concat_ws('=', concat_ws('_', month, '40277'), cast(month_log_h_del_collect as string)))
 ) as c40277,
collect_set(if(month_log_h_list is null or month_log_h_list = 0, null,
 concat_ws('=', concat_ws('_', month, '40278'), cast(month_log_h_list as string)))
 ) as c40278,
collect_set(if(month_log_h_list_by_seqs is null or month_log_h_list_by_seqs = 0, null,
 concat_ws('=', concat_ws('_', month, '40279'), cast(month_log_h_list_by_seqs as string)))
 ) as c40279,
collect_set(if(month_log_h_detail is null or month_log_h_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40280'), cast(month_log_h_detail as string)))
 ) as c40280,
collect_set(if(month_log_h_list_recommend is null or month_log_h_list_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40281'), cast(month_log_h_list_recommend as string)))
 ) as c40281,
collect_set(if(month_log_h_detail_around is null or month_log_h_detail_around = 0, null,
 concat_ws('=', concat_ws('_', month, '40282'), cast(month_log_h_detail_around as string)))
 ) as c40282,
collect_set(if(month_log_h_list_theme is null or month_log_h_list_theme = 0, null,
 concat_ws('=', concat_ws('_', month, '40283'), cast(month_log_h_list_theme as string)))
 ) as c40283,
collect_set(if(month_log_h_lastmin_detail is null or month_log_h_lastmin_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40284'), cast(month_log_h_lastmin_detail as string)))
 ) as c40284,
collect_set(if(month_log_h_lastmin_detail_2 is null or month_log_h_lastmin_detail_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40285'), cast(month_log_h_lastmin_detail_2 as string)))
 ) as c40285,
collect_set(if(month_log_h_lastmin_map is null or month_log_h_lastmin_map = 0, null,
 concat_ws('=', concat_ws('_', month, '40286'), cast(month_log_h_lastmin_map as string)))
 ) as c40286,
collect_set(if(month_log_h_list_hour_room_detail is null or month_log_h_list_hour_room_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40287'), cast(month_log_h_list_hour_room_detail as string)))
 ) as c40287,
collect_set(if(month_log_h_recommend_in_detail is null or month_log_h_recommend_in_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40288'), cast(month_log_h_recommend_in_detail as string)))
 ) as c40288,
collect_set(if(month_log_h_list_hour_room is null or month_log_h_list_hour_room = 0, null,
 concat_ws('=', concat_ws('_', month, '40289'), cast(month_log_h_list_hour_room as string)))
 ) as c40289,
collect_set(if(month_log_h_hour_room_detail is null or month_log_h_hour_room_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40290'), cast(month_log_h_hour_room_detail as string)))
 ) as c40290,
collect_set(if(month_log_h_hour_room_price is null or month_log_h_hour_room_price = 0, null,
 concat_ws('=', concat_ws('_', month, '40291'), cast(month_log_h_hour_room_price as string)))
 ) as c40291,
collect_set(if(month_log_h_detail_price is null or month_log_h_detail_price = 0, null,
 concat_ws('=', concat_ws('_', month, '40292'), cast(month_log_h_detail_price as string)))
 ) as c40292,
collect_set(if(month_log_h_detail_price_2 is null or month_log_h_detail_price_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40293'), cast(month_log_h_detail_price_2 as string)))
 ) as c40293,
collect_set(if(month_log_h_list_promotion is null or month_log_h_list_promotion = 0, null,
 concat_ws('=', concat_ws('_', month, '40294'), cast(month_log_h_list_promotion as string)))
 ) as c40294,
collect_set(if(month_log_h_list_lastmin_price is null or month_log_h_list_lastmin_price = 0, null,
 concat_ws('=', concat_ws('_', month, '40295'), cast(month_log_h_list_lastmin_price as string)))
 ) as c40295,
collect_set(if(month_log_h_search_hot_key is null or month_log_h_search_hot_key = 0, null,
 concat_ws('=', concat_ws('_', month, '40296'), cast(month_log_h_search_hot_key as string)))
 ) as c40296,
collect_set(if(month_log_h_search_hot_key_2 is null or month_log_h_search_hot_key_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40297'), cast(month_log_h_search_hot_key_2 as string)))
 ) as c40297,
collect_set(if(month_log_h_hot_key_suggest is null or month_log_h_hot_key_suggest = 0, null,
 concat_ws('=', concat_ws('_', month, '40298'), cast(month_log_h_hot_key_suggest as string)))
 ) as c40298,
collect_set(if(month_log_h_search_nav is null or month_log_h_search_nav = 0, null,
 concat_ws('=', concat_ws('_', month, '40299'), cast(month_log_h_search_nav as string)))
 ) as c40299,
collect_set(if(month_log_h_time_limit_offer is null or month_log_h_time_limit_offer = 0, null,
 concat_ws('=', concat_ws('_', month, '40300'), cast(month_log_h_time_limit_offer as string)))
 ) as c40300,
collect_set(if(month_log_h_time_limit_offer_2 is null or month_log_h_time_limit_offer_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40301'), cast(month_log_h_time_limit_offer_2 as string)))
 ) as c40301,
collect_set(if(month_log_h_lastmin_count_down is null or month_log_h_lastmin_count_down = 0, null,
 concat_ws('=', concat_ws('_', month, '40302'), cast(month_log_h_lastmin_count_down as string)))
 ) as c40302,
collect_set(if(month_log_h_list_comment is null or month_log_h_list_comment = 0, null,
 concat_ws('=', concat_ws('_', month, '40303'), cast(month_log_h_list_comment as string)))
 ) as c40303,
collect_set(if(month_log_h_list_comment_2 is null or month_log_h_list_comment_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40304'), cast(month_log_h_list_comment_2 as string)))
 ) as c40304,
collect_set(if(month_log_h_comment_detail is null or month_log_h_comment_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40305'), cast(month_log_h_comment_detail as string)))
 ) as c40305,
collect_set(if(month_log_h_comment_detail_2 is null or month_log_h_comment_detail_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40306'), cast(month_log_h_comment_detail_2 as string)))
 ) as c40306,
collect_set(if(month_log_h_edit_comment is null or month_log_h_edit_comment = 0, null,
 concat_ws('=', concat_ws('_', month, '40307'), cast(month_log_h_edit_comment as string)))
 ) as c40307,
collect_set(if(month_log_h_add_comment is null or month_log_h_add_comment = 0, null,
 concat_ws('=', concat_ws('_', month, '40308'), cast(month_log_h_add_comment as string)))
 ) as c40308,
collect_set(if(month_log_h_history is null or month_log_h_history = 0, null,
 concat_ws('=', concat_ws('_', month, '40309'), cast(month_log_h_history as string)))
 ) as c40309,
collect_set(if(month_log_h_history2 is null or month_log_h_history2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40310'), cast(month_log_h_history2 as string)))
 ) as c40310,
collect_set(if(month_log_h_fill_order is null or month_log_h_fill_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40311'), cast(month_log_h_fill_order as string)))
 ) as c40311,
collect_set(if(month_log_h_fill_hour_room_order is null or month_log_h_fill_hour_room_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40312'), cast(month_log_h_fill_hour_room_order as string)))
 ) as c40312,
collect_set(if(month_log_h_coupon is null or month_log_h_coupon = 0, null,
 concat_ws('=', concat_ws('_', month, '40313'), cast(month_log_h_coupon as string)))
 ) as c40313,
collect_set(if(month_log_h_list_coupon is null or month_log_h_list_coupon = 0, null,
 concat_ws('=', concat_ws('_', month, '40314'), cast(month_log_h_list_coupon as string)))
 ) as c40314,
collect_set(if(month_log_h_list_voucher is null or month_log_h_list_voucher = 0, null,
 concat_ws('=', concat_ws('_', month, '40315'), cast(month_log_h_list_voucher as string)))
 ) as c40315,
collect_set(if(month_log_h_coupon_verify is null or month_log_h_coupon_verify = 0, null,
 concat_ws('=', concat_ws('_', month, '40316'), cast(month_log_h_coupon_verify as string)))
 ) as c40316,
collect_set(if(month_log_h_list_member_card is null or month_log_h_list_member_card = 0, null,
 concat_ws('=', concat_ws('_', month, '40317'), cast(month_log_h_list_member_card as string)))
 ) as c40317,
collect_set(if(month_log_h_member_card_info is null or month_log_h_member_card_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40318'), cast(month_log_h_member_card_info as string)))
 ) as c40318,
collect_set(if(month_log_h_submit_order is null or month_log_h_submit_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40319'), cast(month_log_h_submit_order as string)))
 ) as c40319,
collect_set(if(month_log_h_submit_at_once is null or month_log_h_submit_at_once = 0, null,
 concat_ws('=', concat_ws('_', month, '40320'), cast(month_log_h_submit_at_once as string)))
 ) as c40320,
collect_set(if(month_log_h_check_prepay is null or month_log_h_check_prepay = 0, null,
 concat_ws('=', concat_ws('_', month, '40321'), cast(month_log_h_check_prepay as string)))
 ) as c40321,
collect_set(if(month_log_h_check_after_pay is null or month_log_h_check_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40322'), cast(month_log_h_check_after_pay as string)))
 ) as c40322,
collect_set(if(month_log_h_order_detail is null or month_log_h_order_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40323'), cast(month_log_h_order_detail as string)))
 ) as c40323,
collect_set(if(month_log_h_order_detail_verify is null or month_log_h_order_detail_verify = 0, null,
 concat_ws('=', concat_ws('_', month, '40324'), cast(month_log_h_order_detail_verify as string)))
 ) as c40324,
collect_set(if(month_log_h_order_verify is null or month_log_h_order_verify = 0, null,
 concat_ws('=', concat_ws('_', month, '40325'), cast(month_log_h_order_verify as string)))
 ) as c40325,
collect_set(if(month_log_h_share_order is null or month_log_h_share_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40326'), cast(month_log_h_share_order as string)))
 ) as c40326,
collect_set(if(month_log_h_list_order is null or month_log_h_list_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40327'), cast(month_log_h_list_order as string)))
 ) as c40327,
collect_set(if(month_log_h_list_hour_room_order is null or month_log_h_list_hour_room_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40328'), cast(month_log_h_list_hour_room_order as string)))
 ) as c40328,
collect_set(if(month_log_h_link_order is null or month_log_h_link_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40329'), cast(month_log_h_link_order as string)))
 ) as c40329,
collect_set(if(month_log_h_cash_back_req is null or month_log_h_cash_back_req = 0, null,
 concat_ws('=', concat_ws('_', month, '40330'), cast(month_log_h_cash_back_req as string)))
 ) as c40330,
collect_set(if(month_log_h_cash_back is null or month_log_h_cash_back = 0, null,
 concat_ws('=', concat_ws('_', month, '40331'), cast(month_log_h_cash_back as string)))
 ) as c40331,
collect_set(if(month_log_h_modify_order is null or month_log_h_modify_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40332'), cast(month_log_h_modify_order as string)))
 ) as c40332,
collect_set(if(month_log_h_del_order is null or month_log_h_del_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40333'), cast(month_log_h_del_order as string)))
 ) as c40333,
collect_set(if(month_log_h_list_phone_choice is null or month_log_h_list_phone_choice = 0, null,
 concat_ws('=', concat_ws('_', month, '40334'), cast(month_log_h_list_phone_choice as string)))
 ) as c40334,
collect_set(if(month_log_h_call is null or month_log_h_call = 0, null,
 concat_ws('=', concat_ws('_', month, '40335'), cast(month_log_h_call as string)))
 ) as c40335,
collect_set(if(month_log_h_call_direct is null or month_log_h_call_direct = 0, null,
 concat_ws('=', concat_ws('_', month, '40336'), cast(month_log_h_call_direct as string)))
 ) as c40336,
collect_set(if(month_log_h_list_question is null or month_log_h_list_question = 0, null,
 concat_ws('=', concat_ws('_', month, '40337'), cast(month_log_h_list_question as string)))
 ) as c40337,
collect_set(if(month_log_h_list_question2 is null or month_log_h_list_question2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40338'), cast(month_log_h_list_question2 as string)))
 ) as c40338,
collect_set(if(month_log_h_location is null or month_log_h_location = 0, null,
 concat_ws('=', concat_ws('_', month, '40339'), cast(month_log_h_location as string)))
 ) as c40339,
collect_set(if(month_log_h_location_2 is null or month_log_h_location_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40340'), cast(month_log_h_location_2 as string)))
 ) as c40340,
collect_set(if(month_log_h_check_in_before is null or month_log_h_check_in_before = 0, null,
 concat_ws('=', concat_ws('_', month, '40341'), cast(month_log_h_check_in_before as string)))
 ) as c40341,
collect_set(if(month_log_h_ads_recommend is null or month_log_h_ads_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40342'), cast(month_log_h_ads_recommend as string)))
 ) as c40342,
collect_set(if(month_log_h_update_city is null or month_log_h_update_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40343'), cast(month_log_h_update_city as string)))
 ) as c40343,
collect_set(if(month_log_h_log4chain is null or month_log_h_log4chain = 0, null,
 concat_ws('=', concat_ws('_', month, '40344'), cast(month_log_h_log4chain as string)))
 ) as c40344,
collect_set(if(month_log_h_global_info is null or month_log_h_global_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40345'), cast(month_log_h_global_info as string)))
 ) as c40345,
collect_set(if(month_log_h_city_suggest is null or month_log_h_city_suggest = 0, null,
 concat_ws('=', concat_ws('_', month, '40346'), cast(month_log_h_city_suggest as string)))
 ) as c40346,
collect_set(if(month_log_h_recommend is null or month_log_h_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40347'), cast(month_log_h_recommend as string)))
 ) as c40347,
collect_set(if(month_log_h_hour_room_city is null or month_log_h_hour_room_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40348'), cast(month_log_h_hour_room_city as string)))
 ) as c40348,
collect_set(if(month_log_h_city_time_zone is null or month_log_h_city_time_zone = 0, null,
 concat_ws('=', concat_ws('_', month, '40349'), cast(month_log_h_city_time_zone as string)))
 ) as c40349,
collect_set(if(month_log_h_lastmin_city is null or month_log_h_lastmin_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40350'), cast(month_log_h_lastmin_city as string)))
 ) as c40350,
collect_set(if(month_log_h_chain_get_lua is null or month_log_h_chain_get_lua = 0, null,
 concat_ws('=', concat_ws('_', month, '40351'), cast(month_log_h_chain_get_lua as string)))
 ) as c40351,
collect_set(if(month_log_h_allow_lastmin_push is null or month_log_h_allow_lastmin_push = 0, null,
 concat_ws('=', concat_ws('_', month, '40352'), cast(month_log_h_allow_lastmin_push as string)))
 ) as c40352,
collect_set(if(month_log_h_close_lastmin_push is null or month_log_h_close_lastmin_push = 0, null,
 concat_ws('=', concat_ws('_', month, '40353'), cast(month_log_h_close_lastmin_push as string)))
 ) as c40353,
collect_set(if(month_log_h_ensure_trigger is null or month_log_h_ensure_trigger = 0, null,
 concat_ws('=', concat_ws('_', month, '40354'), cast(month_log_h_ensure_trigger as string)))
 ) as c40354,
collect_set(if(month_log_h_get_schema is null or month_log_h_get_schema = 0, null,
 concat_ws('=', concat_ws('_', month, '40355'), cast(month_log_h_get_schema as string)))
 ) as c40355,
collect_set(if(month_log_h_get_img is null or month_log_h_get_img = 0, null,
 concat_ws('=', concat_ws('_', month, '40356'), cast(month_log_h_get_img as string)))
 ) as c40356,
collect_set(if(month_log_h_get_img_2 is null or month_log_h_get_img_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40357'), cast(month_log_h_get_img_2 as string)))
 ) as c40357,
collect_set(if(month_log_h_list_operate is null or month_log_h_list_operate = 0, null,
 concat_ws('=', concat_ws('_', month, '40358'), cast(month_log_h_list_operate as string)))
 ) as c40358

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_hotel_month
WHERE is_not_null(user_id)
) t12
ON t.user_id = t12.user_id

WHERE
    t12.month >= t.begin_month and t12.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 4.5: 日志图-团购

CREATE VIEW tsfm.log_group_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_g_tab is null or month_log_g_tab = 0, null,
 concat_ws('=', concat_ws('_', month, '40359'), cast(month_log_g_tab as string)))
 ) as c40359,
collect_set(if(month_log_g_tab_2 is null or month_log_g_tab_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40360'), cast(month_log_g_tab_2 as string)))
 ) as c40360,
collect_set(if(month_log_g_lastmin is null or month_log_g_lastmin = 0, null,
 concat_ws('=', concat_ws('_', month, '40361'), cast(month_log_g_lastmin as string)))
 ) as c40361,
collect_set(if(month_log_g_list is null or month_log_g_list = 0, null,
 concat_ws('=', concat_ws('_', month, '40362'), cast(month_log_g_list as string)))
 ) as c40362,
collect_set(if(month_log_g_list_2 is null or month_log_g_list_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40363'), cast(month_log_g_list_2 as string)))
 ) as c40363,
collect_set(if(month_log_g_list_3 is null or month_log_g_list_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40364'), cast(month_log_g_list_3 as string)))
 ) as c40364,
collect_set(if(month_log_g_list_4 is null or month_log_g_list_4 = 0, null,
 concat_ws('=', concat_ws('_', month, '40365'), cast(month_log_g_list_4 as string)))
 ) as c40365,
collect_set(if(month_log_g_list_5 is null or month_log_g_list_5 = 0, null,
 concat_ws('=', concat_ws('_', month, '40366'), cast(month_log_g_list_5 as string)))
 ) as c40366,
collect_set(if(month_log_g_list_6 is null or month_log_g_list_6 = 0, null,
 concat_ws('=', concat_ws('_', month, '40367'), cast(month_log_g_list_6 as string)))
 ) as c40367,
collect_set(if(month_log_g_search_suggest is null or month_log_g_search_suggest = 0, null,
 concat_ws('=', concat_ws('_', month, '40368'), cast(month_log_g_search_suggest as string)))
 ) as c40368,
collect_set(if(month_log_g_search_list is null or month_log_g_search_list = 0, null,
 concat_ws('=', concat_ws('_', month, '40369'), cast(month_log_g_search_list as string)))
 ) as c40369,
collect_set(if(month_log_g_search_hot_key is null or month_log_g_search_hot_key = 0, null,
 concat_ws('=', concat_ws('_', month, '40370'), cast(month_log_g_search_hot_key as string)))
 ) as c40370,
collect_set(if(month_log_g_list_and_search is null or month_log_g_list_and_search = 0, null,
 concat_ws('=', concat_ws('_', month, '40371'), cast(month_log_g_list_and_search as string)))
 ) as c40371,
collect_set(if(month_log_g_choose_city is null or month_log_g_choose_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40372'), cast(month_log_g_choose_city as string)))
 ) as c40372,
collect_set(if(month_log_g_suggest_city is null or month_log_g_suggest_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40373'), cast(month_log_g_suggest_city as string)))
 ) as c40373,
collect_set(if(month_log_g_view_history is null or month_log_g_view_history = 0, null,
 concat_ws('=', concat_ws('_', month, '40374'), cast(month_log_g_view_history as string)))
 ) as c40374,
collect_set(if(month_log_g_list_hour_room is null or month_log_g_list_hour_room = 0, null,
 concat_ws('=', concat_ws('_', month, '40375'), cast(month_log_g_list_hour_room as string)))
 ) as c40375,
collect_set(if(month_log_g_nearby_recommend is null or month_log_g_nearby_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40376'), cast(month_log_g_nearby_recommend as string)))
 ) as c40376,
collect_set(if(month_log_g_nearby_recommend_2 is null or month_log_g_nearby_recommend_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40377'), cast(month_log_g_nearby_recommend_2 as string)))
 ) as c40377,
collect_set(if(month_log_g_other_recommend is null or month_log_g_other_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40378'), cast(month_log_g_other_recommend as string)))
 ) as c40378,
collect_set(if(month_log_g_detail is null or month_log_g_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40379'), cast(month_log_g_detail as string)))
 ) as c40379,
collect_set(if(month_log_g_detail_2 is null or month_log_g_detail_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40380'), cast(month_log_g_detail_2 as string)))
 ) as c40380,
collect_set(if(month_log_g_detail_3 is null or month_log_g_detail_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40381'), cast(month_log_g_detail_3 as string)))
 ) as c40381,
collect_set(if(month_log_g_list_collection is null or month_log_g_list_collection = 0, null,
 concat_ws('=', concat_ws('_', month, '40382'), cast(month_log_g_list_collection as string)))
 ) as c40382,
collect_set(if(month_log_g_collect is null or month_log_g_collect = 0, null,
 concat_ws('=', concat_ws('_', month, '40383'), cast(month_log_g_collect as string)))
 ) as c40383,
collect_set(if(month_log_g_merge_order is null or month_log_g_merge_order = 0, null,
 concat_ws('=', concat_ws('_', month, '40384'), cast(month_log_g_merge_order as string)))
 ) as c40384,
collect_set(if(month_log_g_list_voucher is null or month_log_g_list_voucher = 0, null,
 concat_ws('=', concat_ws('_', month, '40385'), cast(month_log_g_list_voucher as string)))
 ) as c40385,
collect_set(if(month_log_g_submit is null or month_log_g_submit = 0, null,
 concat_ws('=', concat_ws('_', month, '40386'), cast(month_log_g_submit as string)))
 ) as c40386,
collect_set(if(month_log_g_prepay_detail is null or month_log_g_prepay_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40387'), cast(month_log_g_prepay_detail as string)))
 ) as c40387,
collect_set(if(month_log_g_recommend_after_pay is null or month_log_g_recommend_after_pay = 0, null,
 concat_ws('=', concat_ws('_', month, '40388'), cast(month_log_g_recommend_after_pay as string)))
 ) as c40388,
collect_set(if(month_log_g_activity is null or month_log_g_activity = 0, null,
 concat_ws('=', concat_ws('_', month, '40389'), cast(month_log_g_activity as string)))
 ) as c40389,
collect_set(if(month_log_g_category_img is null or month_log_g_category_img = 0, null,
 concat_ws('=', concat_ws('_', month, '40390'), cast(month_log_g_category_img as string)))
 ) as c40390,
collect_set(if(month_log_g_location is null or month_log_g_location = 0, null,
 concat_ws('=', concat_ws('_', month, '40391'), cast(month_log_g_location as string)))
 ) as c40391,
collect_set(if(month_log_g_layer_detail is null or month_log_g_layer_detail = 0, null,
 concat_ws('=', concat_ws('_', month, '40392'), cast(month_log_g_layer_detail as string)))
 ) as c40392

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_group_month
WHERE is_not_null(user_id)
) t13
ON t.user_id = t13.user_id

WHERE
    t13.month >= t.begin_month and t13.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 4.6: 日志图-其他

CREATE VIEW tsfm.log_other_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_o_ios_monitor is null or month_log_o_ios_monitor = 0, null,
 concat_ws('=', concat_ws('_', month, '40393'), cast(month_log_o_ios_monitor as string)))
 ) as c40393,
collect_set(if(month_log_o_ios_bug_patch is null or month_log_o_ios_bug_patch = 0, null,
 concat_ws('=', concat_ws('_', month, '40394'), cast(month_log_o_ios_bug_patch as string)))
 ) as c40394,
collect_set(if(month_log_o_ios_receipt is null or month_log_o_ios_receipt = 0, null,
 concat_ws('=', concat_ws('_', month, '40395'), cast(month_log_o_ios_receipt as string)))
 ) as c40395,
collect_set(if(month_log_o_ios_get_msg is null or month_log_o_ios_get_msg = 0, null,
 concat_ws('=', concat_ws('_', month, '40396'), cast(month_log_o_ios_get_msg as string)))
 ) as c40396,
collect_set(if(month_log_o_android_monitor is null or month_log_o_android_monitor = 0, null,
 concat_ws('=', concat_ws('_', month, '40397'), cast(month_log_o_android_monitor as string)))
 ) as c40397,
collect_set(if(month_log_o_android_error is null or month_log_o_android_error = 0, null,
 concat_ws('=', concat_ws('_', month, '40398'), cast(month_log_o_android_error as string)))
 ) as c40398,
collect_set(if(month_log_o_home_banner is null or month_log_o_home_banner = 0, null,
 concat_ws('=', concat_ws('_', month, '40399'), cast(month_log_o_home_banner as string)))
 ) as c40399,
collect_set(if(month_log_o_splash_ad is null or month_log_o_splash_ad = 0, null,
 concat_ws('=', concat_ws('_', month, '40400'), cast(month_log_o_splash_ad as string)))
 ) as c40400,
collect_set(if(month_log_o_home_animation is null or month_log_o_home_animation = 0, null,
 concat_ws('=', concat_ws('_', month, '40401'), cast(month_log_o_home_animation as string)))
 ) as c40401,
collect_set(if(month_log_o_home_pop_up is null or month_log_o_home_pop_up = 0, null,
 concat_ws('=', concat_ws('_', month, '40402'), cast(month_log_o_home_pop_up as string)))
 ) as c40402,
collect_set(if(month_log_o_show_toast is null or month_log_o_show_toast = 0, null,
 concat_ws('=', concat_ws('_', month, '40403'), cast(month_log_o_show_toast as string)))
 ) as c40403,
collect_set(if(month_log_o_show_toast_2 is null or month_log_o_show_toast_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40404'), cast(month_log_o_show_toast_2 as string)))
 ) as c40404,
collect_set(if(month_log_o_red_point is null or month_log_o_red_point = 0, null,
 concat_ws('=', concat_ws('_', month, '40405'), cast(month_log_o_red_point as string)))
 ) as c40405,
collect_set(if(month_log_o_red_point_2 is null or month_log_o_red_point_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40406'), cast(month_log_o_red_point_2 as string)))
 ) as c40406,
collect_set(if(month_log_o_corner_mark is null or month_log_o_corner_mark = 0, null,
 concat_ws('=', concat_ws('_', month, '40407'), cast(month_log_o_corner_mark as string)))
 ) as c40407,
collect_set(if(month_log_o_click_recommend is null or month_log_o_click_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40408'), cast(month_log_o_click_recommend as string)))
 ) as c40408,
collect_set(if(month_log_o_notify_column is null or month_log_o_notify_column = 0, null,
 concat_ws('=', concat_ws('_', month, '40409'), cast(month_log_o_notify_column as string)))
 ) as c40409,
collect_set(if(month_log_o_search_doc is null or month_log_o_search_doc = 0, null,
 concat_ws('=', concat_ws('_', month, '40410'), cast(month_log_o_search_doc as string)))
 ) as c40410,
collect_set(if(month_log_o_biz_recommend is null or month_log_o_biz_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40411'), cast(month_log_o_biz_recommend as string)))
 ) as c40411,
collect_set(if(month_log_o_biz_recommend_2 is null or month_log_o_biz_recommend_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40412'), cast(month_log_o_biz_recommend_2 as string)))
 ) as c40412,
collect_set(if(month_log_o_home_recommend is null or month_log_o_home_recommend = 0, null,
 concat_ws('=', concat_ws('_', month, '40413'), cast(month_log_o_home_recommend as string)))
 ) as c40413,
collect_set(if(month_log_o_get_pay_info is null or month_log_o_get_pay_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40414'), cast(month_log_o_get_pay_info as string)))
 ) as c40414,
collect_set(if(month_log_o_change_order_num is null or month_log_o_change_order_num = 0, null,
 concat_ws('=', concat_ws('_', month, '40415'), cast(month_log_o_change_order_num as string)))
 ) as c40415,
collect_set(if(month_log_o_notice_dbt is null or month_log_o_notice_dbt = 0, null,
 concat_ws('=', concat_ws('_', month, '40416'), cast(month_log_o_notice_dbt as string)))
 ) as c40416,
collect_set(if(month_log_o_get_location is null or month_log_o_get_location = 0, null,
 concat_ws('=', concat_ws('_', month, '40417'), cast(month_log_o_get_location as string)))
 ) as c40417,
collect_set(if(month_log_o_get_location_failed is null or month_log_o_get_location_failed = 0, null,
 concat_ws('=', concat_ws('_', month, '40418'), cast(month_log_o_get_location_failed as string)))
 ) as c40418,
collect_set(if(month_log_o_weather_service is null or month_log_o_weather_service = 0, null,
 concat_ws('=', concat_ws('_', month, '40419'), cast(month_log_o_weather_service as string)))
 ) as c40419,
collect_set(if(month_log_o_upload_location is null or month_log_o_upload_location = 0, null,
 concat_ws('=', concat_ws('_', month, '40420'), cast(month_log_o_upload_location as string)))
 ) as c40420,
collect_set(if(month_log_o_get_address_code is null or month_log_o_get_address_code = 0, null,
 concat_ws('=', concat_ws('_', month, '40421'), cast(month_log_o_get_address_code as string)))
 ) as c40421,
collect_set(if(month_log_o_into_map is null or month_log_o_into_map = 0, null,
 concat_ws('=', concat_ws('_', month, '40422'), cast(month_log_o_into_map as string)))
 ) as c40422,
collect_set(if(month_log_o_into_map_2 is null or month_log_o_into_map_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40423'), cast(month_log_o_into_map_2 as string)))
 ) as c40423,
collect_set(if(month_log_o_list_city is null or month_log_o_list_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40424'), cast(month_log_o_list_city as string)))
 ) as c40424,
collect_set(if(month_log_o_update_hot_city is null or month_log_o_update_hot_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40425'), cast(month_log_o_update_hot_city as string)))
 ) as c40425,
collect_set(if(month_log_o_hot_arrv_city is null or month_log_o_hot_arrv_city = 0, null,
 concat_ws('=', concat_ws('_', month, '40426'), cast(month_log_o_hot_arrv_city as string)))
 ) as c40426,
collect_set(if(month_log_o_switch_home is null or month_log_o_switch_home = 0, null,
 concat_ws('=', concat_ws('_', month, '40427'), cast(month_log_o_switch_home as string)))
 ) as c40427,
collect_set(if(month_log_o_risk_collect is null or month_log_o_risk_collect = 0, null,
 concat_ws('=', concat_ws('_', month, '40428'), cast(month_log_o_risk_collect as string)))
 ) as c40428,
collect_set(if(month_log_o_risk_info is null or month_log_o_risk_info = 0, null,
 concat_ws('=', concat_ws('_', month, '40429'), cast(month_log_o_risk_info as string)))
 ) as c40429,
collect_set(if(month_log_o_client_abtest is null or month_log_o_client_abtest = 0, null,
 concat_ws('=', concat_ws('_', month, '40430'), cast(month_log_o_client_abtest as string)))
 ) as c40430,
collect_set(if(month_log_o_client_ue is null or month_log_o_client_ue = 0, null,
 concat_ws('=', concat_ws('_', month, '40431'), cast(month_log_o_client_ue as string)))
 ) as c40431,
collect_set(if(month_log_o_client_ue_2 is null or month_log_o_client_ue_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40432'), cast(month_log_o_client_ue_2 as string)))
 ) as c40432,
collect_set(if(month_log_o_trigger_when_home is null or month_log_o_trigger_when_home = 0, null,
 concat_ws('=', concat_ws('_', month, '40433'), cast(month_log_o_trigger_when_home as string)))
 ) as c40433,
collect_set(if(month_log_o_competing_apps is null or month_log_o_competing_apps = 0, null,
 concat_ws('=', concat_ws('_', month, '40434'), cast(month_log_o_competing_apps as string)))
 ) as c40434,
collect_set(if(month_log_o_get_schema is null or month_log_o_get_schema = 0, null,
 concat_ws('=', concat_ws('_', month, '40435'), cast(month_log_o_get_schema as string)))
 ) as c40435,
collect_set(if(month_log_o_list_prefix_num is null or month_log_o_list_prefix_num = 0, null,
 concat_ws('=', concat_ws('_', month, '40436'), cast(month_log_o_list_prefix_num as string)))
 ) as c40436,
collect_set(if(month_log_o_update_holiday is null or month_log_o_update_holiday = 0, null,
 concat_ws('=', concat_ws('_', month, '40437'), cast(month_log_o_update_holiday as string)))
 ) as c40437,
collect_set(if(month_log_o_home_block_display is null or month_log_o_home_block_display = 0, null,
 concat_ws('=', concat_ws('_', month, '40438'), cast(month_log_o_home_block_display as string)))
 ) as c40438,
collect_set(if(month_log_o_service_access is null or month_log_o_service_access = 0, null,
 concat_ws('=', concat_ws('_', month, '40439'), cast(month_log_o_service_access as string)))
 ) as c40439

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_other_month
WHERE is_not_null(user_id)
) t14
ON t.user_id =t14.user_id

WHERE
    t14.month >= t.begin_month and t14.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 4.7: 日志图-未知

CREATE VIEW tsfm.log_unknown_features_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_log_unknown_1 is null or month_log_unknown_1 = 0, null,
 concat_ws('=', concat_ws('_', month, '40440'), cast(month_log_unknown_1 as string)))
 ) as c40440,
collect_set(if(month_log_unknown_2 is null or month_log_unknown_2 = 0, null,
 concat_ws('=', concat_ws('_', month, '40441'), cast(month_log_unknown_2 as string)))
 ) as c40441,
collect_set(if(month_log_unknown_3 is null or month_log_unknown_3 = 0, null,
 concat_ws('=', concat_ws('_', month, '40442'), cast(month_log_unknown_3 as string)))
 ) as c40442,
collect_set(if(month_log_unknown_4 is null or month_log_unknown_4 = 0, null,
 concat_ws('=', concat_ws('_', month, '40443'), cast(month_log_unknown_4 as string)))
 ) as c40443,
collect_set(if(month_log_unknown_5 is null or month_log_unknown_5 = 0, null,
 concat_ws('=', concat_ws('_', month, '40444'), cast(month_log_unknown_5 as string)))
 ) as c40444,
collect_set(if(month_log_unknown_6 is null or month_log_unknown_6 = 0, null,
 concat_ws('=', concat_ws('_', month, '40445'), cast(month_log_unknown_6 as string)))
 ) as c40445,
collect_set(if(month_log_unknown_7 is null or month_log_unknown_7 = 0, null,
 concat_ws('=', concat_ws('_', month, '40446'), cast(month_log_unknown_7 as string)))
 ) as c40446,
collect_set(if(month_log_unknown_8 is null or month_log_unknown_8 = 0, null,
 concat_ws('=', concat_ws('_', month, '40447'), cast(month_log_unknown_8 as string)))
 ) as c40447,
collect_set(if(month_log_unknown_9 is null or month_log_unknown_9 = 0, null,
 concat_ws('=', concat_ws('_', month, '40448'), cast(month_log_unknown_9 as string)))
 ) as c40448,
collect_set(if(month_log_unknown_10 is null or month_log_unknown_10 = 0, null,
 concat_ws('=', concat_ws('_', month, '40449'), cast(month_log_unknown_10 as string)))
 ) as c40449,
collect_set(if(month_log_unknown_11 is null or month_log_unknown_11 = 0, null,
 concat_ws('=', concat_ws('_', month, '40450'), cast(month_log_unknown_11 as string)))
 ) as c40450,
collect_set(if(month_log_unknown_12 is null or month_log_unknown_12 = 0, null,
 concat_ws('=', concat_ws('_', month, '40451'), cast(month_log_unknown_12 as string)))
 ) as c40451,
collect_set(if(month_log_unknown_13 is null or month_log_unknown_13 = 0, null,
 concat_ws('=', concat_ws('_', month, '40452'), cast(month_log_unknown_13 as string)))
 ) as c40452,
collect_set(if(month_log_unknown_14 is null or month_log_unknown_14 = 0, null,
 concat_ws('=', concat_ws('_', month, '40453'), cast(month_log_unknown_14 as string)))
 ) as c40453,
collect_set(if(month_log_unknown_15 is null or month_log_unknown_15 = 0, null,
 concat_ws('=', concat_ws('_', month, '40454'), cast(month_log_unknown_15 as string)))
 ) as c40454,
collect_set(if(month_log_unknown_16 is null or month_log_unknown_16 = 0, null,
 concat_ws('=', concat_ws('_', month, '40455'), cast(month_log_unknown_16 as string)))
 ) as c40455,
collect_set(if(month_log_unknown_17 is null or month_log_unknown_17 = 0, null,
 concat_ws('=', concat_ws('_', month, '40456'), cast(month_log_unknown_17 as string)))
 ) as c40456,
collect_set(if(month_log_unknown_18 is null or month_log_unknown_18 = 0, null,
 concat_ws('=', concat_ws('_', month, '40457'), cast(month_log_unknown_18 as string)))
 ) as c40457,
collect_set(if(month_log_unknown_19 is null or month_log_unknown_19 = 0, null,
 concat_ws('=', concat_ws('_', month, '40458'), cast(month_log_unknown_19 as string)))
 ) as c40458,
collect_set(if(month_log_unknown_20 is null or month_log_unknown_20 = 0, null,
 concat_ws('=', concat_ws('_', month, '40459'), cast(month_log_unknown_20 as string)))
 ) as c40459,
collect_set(if(month_log_unknown_21 is null or month_log_unknown_21 = 0, null,
 concat_ws('=', concat_ws('_', month, '40460'), cast(month_log_unknown_21 as string)))
 ) as c40460,
collect_set(if(month_log_unknown_22 is null or month_log_unknown_22 = 0, null,
 concat_ws('=', concat_ws('_', month, '40461'), cast(month_log_unknown_22 as string)))
 ) as c40461,
collect_set(if(month_log_unknown_23 is null or month_log_unknown_23 = 0, null,
 concat_ws('=', concat_ws('_', month, '40462'), cast(month_log_unknown_23 as string)))
 ) as c40462,
collect_set(if(month_log_unknown_24 is null or month_log_unknown_24 = 0, null,
 concat_ws('=', concat_ws('_', month, '40463'), cast(month_log_unknown_24 as string)))
 ) as c40463,
collect_set(if(month_log_unknown_25 is null or month_log_unknown_25 = 0, null,
 concat_ws('=', concat_ws('_', month, '40464'), cast(month_log_unknown_25 as string)))
 ) as c40464,
collect_set(if(month_log_unknown_26 is null or month_log_unknown_26 = 0, null,
 concat_ws('=', concat_ws('_', month, '40465'), cast(month_log_unknown_26 as string)))
 ) as c40465,
collect_set(if(month_log_unknown_27 is null or month_log_unknown_27 = 0, null,
 concat_ws('=', concat_ws('_', month, '40466'), cast(month_log_unknown_27 as string)))
 ) as c40466,
collect_set(if(month_log_unknown_28 is null or month_log_unknown_28 = 0, null,
 concat_ws('=', concat_ws('_', month, '40467'), cast(month_log_unknown_28 as string)))
 ) as c40467,
collect_set(if(month_log_unknown_29 is null or month_log_unknown_29 = 0, null,
 concat_ws('=', concat_ws('_', month, '40468'), cast(month_log_unknown_29 as string)))
 ) as c40468,
collect_set(if(month_log_unknown_30 is null or month_log_unknown_30 = 0, null,
 concat_ws('=', concat_ws('_', month, '40469'), cast(month_log_unknown_30 as string)))
 ) as c40469,
collect_set(if(month_log_unknown_31 is null or month_log_unknown_31 = 0, null,
 concat_ws('=', concat_ws('_', month, '40470'), cast(month_log_unknown_31 as string)))
 ) as c40470,
collect_set(if(month_log_unknown_32 is null or month_log_unknown_32 = 0, null,
 concat_ws('=', concat_ws('_', month, '40471'), cast(month_log_unknown_32 as string)))
 ) as c40471,
collect_set(if(month_log_unknown_33 is null or month_log_unknown_33 = 0, null,
 concat_ws('=', concat_ws('_', month, '40472'), cast(month_log_unknown_33 as string)))
 ) as c40472,
collect_set(if(month_log_unknown_34 is null or month_log_unknown_34 = 0, null,
 concat_ws('=', concat_ws('_', month, '40473'), cast(month_log_unknown_34 as string)))
 ) as c40473,
collect_set(if(month_log_unknown_35 is null or month_log_unknown_35 = 0, null,
 concat_ws('=', concat_ws('_', month, '40474'), cast(month_log_unknown_35 as string)))
 ) as c40474,
collect_set(if(month_log_unknown_36 is null or month_log_unknown_36 = 0, null,
 concat_ws('=', concat_ws('_', month, '40475'), cast(month_log_unknown_36 as string)))
 ) as c40475,
collect_set(if(month_log_unknown_37 is null or month_log_unknown_37 = 0, null,
 concat_ws('=', concat_ws('_', month, '40476'), cast(month_log_unknown_37 as string)))
 ) as c40476,
collect_set(if(month_log_unknown_38 is null or month_log_unknown_38 = 0, null,
 concat_ws('=', concat_ws('_', month, '40477'), cast(month_log_unknown_38 as string)))
 ) as c40477,
collect_set(if(month_log_unknown_39 is null or month_log_unknown_39 = 0, null,
 concat_ws('=', concat_ws('_', month, '40478'), cast(month_log_unknown_39 as string)))
 ) as c40478,
collect_set(if(month_log_unknown_40 is null or month_log_unknown_40 = 0, null,
 concat_ws('=', concat_ws('_', month, '40479'), cast(month_log_unknown_40 as string)))
 ) as c40479,
collect_set(if(month_log_unknown_41 is null or month_log_unknown_41 = 0, null,
 concat_ws('=', concat_ws('_', month, '40480'), cast(month_log_unknown_41 as string)))
 ) as c40480,
collect_set(if(month_log_unknown_42 is null or month_log_unknown_42 = 0, null,
 concat_ws('=', concat_ws('_', month, '40481'), cast(month_log_unknown_42 as string)))
 ) as c40481,
collect_set(if(month_log_unknown_43 is null or month_log_unknown_43 = 0, null,
 concat_ws('=', concat_ws('_', month, '40482'), cast(month_log_unknown_43 as string)))
 ) as c40482,
collect_set(if(month_log_unknown_44 is null or month_log_unknown_44 = 0, null,
 concat_ws('=', concat_ws('_', month, '40483'), cast(month_log_unknown_44 as string)))
 ) as c40483,
collect_set(if(month_log_unknown_45 is null or month_log_unknown_45 = 0, null,
 concat_ws('=', concat_ws('_', month, '40484'), cast(month_log_unknown_45 as string)))
 ) as c40484,
collect_set(if(month_log_unknown_46 is null or month_log_unknown_46 = 0, null,
 concat_ws('=', concat_ws('_', month, '40485'), cast(month_log_unknown_46 as string)))
 ) as c40485,
collect_set(if(month_log_unknown_47 is null or month_log_unknown_47 = 0, null,
 concat_ws('=', concat_ws('_', month, '40486'), cast(month_log_unknown_47 as string)))
 ) as c40486,
collect_set(if(month_log_unknown_48 is null or month_log_unknown_48 = 0, null,
 concat_ws('=', concat_ws('_', month, '40487'), cast(month_log_unknown_48 as string)))
 ) as c40487,
collect_set(if(month_log_unknown_49 is null or month_log_unknown_49 = 0, null,
 concat_ws('=', concat_ws('_', month, '40488'), cast(month_log_unknown_49 as string)))
 ) as c40488,
collect_set(if(month_log_unknown_50 is null or month_log_unknown_50 = 0, null,
 concat_ws('=', concat_ws('_', month, '40489'), cast(month_log_unknown_50 as string)))
 ) as c40489,
collect_set(if(month_log_unknown_51 is null or month_log_unknown_51 = 0, null,
 concat_ws('=', concat_ws('_', month, '40490'), cast(month_log_unknown_51 as string)))
 ) as c40490,
collect_set(if(month_log_unknown_52 is null or month_log_unknown_52 = 0, null,
 concat_ws('=', concat_ws('_', month, '40491'), cast(month_log_unknown_52 as string)))
 ) as c40491,
collect_set(if(month_log_unknown_53 is null or month_log_unknown_53 = 0, null,
 concat_ws('=', concat_ws('_', month, '40492'), cast(month_log_unknown_53 as string)))
 ) as c40492,
collect_set(if(month_log_unknown_54 is null or month_log_unknown_54 = 0, null,
 concat_ws('=', concat_ws('_', month, '40493'), cast(month_log_unknown_54 as string)))
 ) as c40493,
collect_set(if(month_log_unknown_55 is null or month_log_unknown_55 = 0, null,
 concat_ws('=', concat_ws('_', month, '40494'), cast(month_log_unknown_55 as string)))
 ) as c40494,
collect_set(if(month_log_unknown_56 is null or month_log_unknown_56 = 0, null,
 concat_ws('=', concat_ws('_', month, '40495'), cast(month_log_unknown_56 as string)))
 ) as c40495,
collect_set(if(month_log_unknown_57 is null or month_log_unknown_57 = 0, null,
 concat_ws('=', concat_ws('_', month, '40496'), cast(month_log_unknown_57 as string)))
 ) as c40496,
collect_set(if(month_log_unknown_58 is null or month_log_unknown_58 = 0, null,
 concat_ws('=', concat_ws('_', month, '40497'), cast(month_log_unknown_58 as string)))
 ) as c40497,
collect_set(if(month_log_unknown_59 is null or month_log_unknown_59 = 0, null,
 concat_ws('=', concat_ws('_', month, '40498'), cast(month_log_unknown_59 as string)))
 ) as c40498,
collect_set(if(month_log_unknown_60 is null or month_log_unknown_60 = 0, null,
 concat_ws('=', concat_ws('_', month, '40499'), cast(month_log_unknown_60 as string)))
 ) as c40499,
collect_set(if(month_log_unknown_61 is null or month_log_unknown_61 = 0, null,
 concat_ws('=', concat_ws('_', month, '40500'), cast(month_log_unknown_61 as string)))
 ) as c40500,
collect_set(if(month_log_unknown_62 is null or month_log_unknown_62 = 0, null,
 concat_ws('=', concat_ws('_', month, '40501'), cast(month_log_unknown_62 as string)))
 ) as c40501,
collect_set(if(month_log_unknown_63 is null or month_log_unknown_63 = 0, null,
 concat_ws('=', concat_ws('_', month, '40502'), cast(month_log_unknown_63 as string)))
 ) as c40502

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.log_action_count_unknown_month
WHERE is_not_null(user_id)
) t15
ON t.user_id = t15.user_id

WHERE
    t15.month >= t.begin_month and t15.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

---------------------------------------------------------------------------------------------------

-- Step 5.1: 拿去花-借款

CREATE VIEW tsfm.ious_loan_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_ious_total_amount is null or month_ious_total_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31001'), cast(month_ious_total_amount as string)))
 ) as c31001,
collect_set(if(month_ious_total_count is null or month_ious_total_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31002'), cast(month_ious_total_count as string)))
 ) as c31002,
collect_set(if(month_ious_avg_pay_order_delta_time is null or month_ious_avg_pay_order_delta_time = 0, null,
 concat_ws('=', concat_ws('_', month, '31003'), cast(month_ious_avg_pay_order_delta_time as string)))
 ) as c31003,
collect_set(if(month_ious_avg_loan_term is null or month_ious_avg_loan_term = 0, null,
 concat_ws('=', concat_ws('_', month, '31004'), cast(month_ious_avg_loan_term as string)))
 ) as c31004,
collect_set(if(month_ious_term_1_amount is null or month_ious_term_1_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31005'), cast(month_ious_term_1_amount as string)))
 ) as c31005,
collect_set(if(month_ious_term_1_count is null or month_ious_term_1_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31006'), cast(month_ious_term_1_count as string)))
 ) as c31006,
collect_set(if(month_ious_term_3_amount is null or month_ious_term_3_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31007'), cast(month_ious_term_3_amount as string)))
 ) as c31007,
collect_set(if(month_ious_term_3_count is null or month_ious_term_3_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31008'), cast(month_ious_term_3_count as string)))
 ) as c31008,
collect_set(if(month_ious_term_6_amount is null or month_ious_term_6_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31009'), cast(month_ious_term_6_amount as string)))
 ) as c31009,
collect_set(if(month_ious_term_6_count is null or month_ious_term_6_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31010'), cast(month_ious_term_6_count as string)))
 ) as c31010,
collect_set(if(month_ious_term_9_amount is null or month_ious_term_9_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31011'), cast(month_ious_term_9_amount as string)))
 ) as c31011,
collect_set(if(month_ious_term_9_count is null or month_ious_term_9_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31012'), cast(month_ious_term_9_count as string)))
 ) as c31012,
collect_set(if(month_ious_term_12_amount is null or month_ious_term_12_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31013'), cast(month_ious_term_12_amount as string)))
 ) as c31013,
collect_set(if(month_ious_term_12_count is null or month_ious_term_12_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31014'), cast(month_ious_term_12_count as string)))
 ) as c31014,
collect_set(if(month_ious_flight_amount is null or month_ious_flight_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31015'), cast(month_ious_flight_amount as string)))
 ) as c31015,
collect_set(if(month_ious_flight_count is null or month_ious_flight_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31016'), cast(month_ious_flight_count as string)))
 ) as c31016,
collect_set(if(month_ious_bus_amount is null or month_ious_bus_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31017'), cast(month_ious_bus_amount as string)))
 ) as c31017,
collect_set(if(month_ious_bus_count is null or month_ious_bus_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31018'), cast(month_ious_bus_count as string)))
 ) as c31018,
collect_set(if(month_ious_ticket_amount is null or month_ious_ticket_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31019'), cast(month_ious_ticket_amount as string)))
 ) as c31019,
collect_set(if(month_ious_ticket_count is null or month_ious_ticket_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31020'), cast(month_ious_ticket_count as string)))
 ) as c31020,
collect_set(if(month_ious_vacation_amount is null or month_ious_vacation_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31021'), cast(month_ious_vacation_amount as string)))
 ) as c31021,
collect_set(if(month_ious_vacation_count is null or month_ious_vacation_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31022'), cast(month_ious_vacation_count as string)))
 ) as c31022,
collect_set(if(month_ious_hotel_amount is null or month_ious_hotel_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31023'), cast(month_ious_hotel_amount as string)))
 ) as c31023,
collect_set(if(month_ious_hotel_count is null or month_ious_hotel_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31024'), cast(month_ious_hotel_count as string)))
 ) as c31024,
collect_set(if(month_ious_groupon_amount is null or month_ious_groupon_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31025'), cast(month_ious_groupon_amount as string)))
 ) as c31025,
collect_set(if(month_ious_groupon_count is null or month_ious_groupon_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31026'), cast(month_ious_groupon_count as string)))
 ) as c31026,
collect_set(if(month_ious_train_amount is null or month_ious_train_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31027'), cast(month_ious_train_amount as string)))
 ) as c31027,
collect_set(if(month_ious_train_count is null or month_ious_train_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31028'), cast(month_ious_train_count as string)))
 ) as c31028,
collect_set(if(month_ious_zuche_amount is null or month_ious_zuche_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31029'), cast(month_ious_zuche_amount as string)))
 ) as c31029,
collect_set(if(month_ious_zuche_count is null or month_ious_zuche_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31030'), cast(month_ious_zuche_count as string)))
 ) as c31030,
collect_set(if(month_ious_avg_rate is null or month_ious_avg_rate = 0, null,
 concat_ws('=', concat_ws('_', month, '31031'), cast(month_ious_avg_rate as string)))
 ) as c31031,
collect_set(if(month_ious_normal_amount is null or month_ious_normal_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31032'), cast(month_ious_normal_amount as string)))
 ) as c31032,
collect_set(if(month_ious_normal_count is null or month_ious_normal_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31033'), cast(month_ious_normal_count as string)))
 ) as c31033,
collect_set(if(month_ious_qianzhi_amount is null or month_ious_qianzhi_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31034'), cast(month_ious_qianzhi_amount as string)))
 ) as c31034,
collect_set(if(month_ious_qianzhi_count is null or month_ious_qianzhi_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31035'), cast(month_ious_qianzhi_count as string)))
 ) as c31035,
collect_set(if(month_ious_shanzhu_amount is null or month_ious_shanzhu_amount = 0, null,
 concat_ws('=', concat_ws('_', month, '31036'), cast(month_ious_shanzhu_amount as string)))
 ) as c31036,
collect_set(if(month_ious_shanzhu_count is null or month_ious_shanzhu_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31037'), cast(month_ious_shanzhu_count as string)))
 ) as c31037,
collect_set(if(month_ious_daytime_count is null or month_ious_daytime_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31038'), cast(month_ious_daytime_count as string)))
 ) as c31038,
collect_set(if(month_ious_night_count is null or month_ious_night_count = 0, null,
 concat_ws('=', concat_ws('_', month, '31039'), cast(month_ious_night_count as string)))
 ) as c31039

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.ious_loan_uid_month
WHERE is_not_null(user_id)
) t16
ON t.user_id = t16.user_id

WHERE
    t16.month >= t.begin_month and t16.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 5.2: 拿去花-还款

CREATE VIEW tsfm.ious_repay_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_ious_total_repay_count is null or month_ious_total_repay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32001'), cast(month_ious_total_repay_count as string)))
 ) as c32001,
collect_set(if(month_ious_total_repay_prcp_amt is null or month_ious_total_repay_prcp_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32002'), cast(month_ious_total_repay_prcp_amt as string)))
 ) as c32002,
collect_set(if(month_ious_total_repay_int_amt is null or month_ious_total_repay_int_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32003'), cast(month_ious_total_repay_int_amt as string)))
 ) as c32003,
collect_set(if(month_ious_total_repay_fee_amt is null or month_ious_total_repay_fee_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32004'), cast(month_ious_total_repay_fee_amt as string)))
 ) as c32004,
collect_set(if(month_ious_total_repay_fine_amt is null or month_ious_total_repay_fine_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32005'), cast(month_ious_total_repay_fine_amt as string)))
 ) as c32005,
collect_set(if(month_ious_total_repay_spreads_amt is null or month_ious_total_repay_spreads_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32006'), cast(month_ious_total_repay_spreads_amt as string)))
 ) as c32006,
collect_set(if(month_ious_inprepay_count is null or month_ious_inprepay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32007'), cast(month_ious_inprepay_count as string)))
 ) as c32007,
collect_set(if(month_ious_bankacc_count is null or month_ious_bankacc_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32008'), cast(month_ious_bankacc_count as string)))
 ) as c32008,
collect_set(if(month_ious_success_count is null or month_ious_success_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32009'), cast(month_ious_success_count as string)))
 ) as c32009,
collect_set(if(month_ious_duplicate_count is null or month_ious_duplicate_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32010'), cast(month_ious_duplicate_count as string)))
 ) as c32010,
collect_set(if(month_ious_close_count is null or month_ious_close_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32011'), cast(month_ious_close_count as string)))
 ) as c32011,
collect_set(if(month_ious_undo_count is null or month_ious_undo_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32012'), cast(month_ious_undo_count as string)))
 ) as c32012,
collect_set(if(month_ious_normal_count is null or month_ious_normal_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32013'), cast(month_ious_normal_count as string)))
 ) as c32013,
collect_set(if(month_ious_normal_prcp_amt is null or month_ious_normal_prcp_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32014'), cast(month_ious_normal_prcp_amt as string)))
 ) as c32014,
collect_set(if(month_ious_normal_int_amt is null or month_ious_normal_int_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32015'), cast(month_ious_normal_int_amt as string)))
 ) as c32015,
collect_set(if(month_ious_normal_fee_amt is null or month_ious_normal_fee_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32016'), cast(month_ious_normal_fee_amt as string)))
 ) as c32016,
collect_set(if(month_ious_normal_fine_amt is null or month_ious_normal_fine_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32017'), cast(month_ious_normal_fine_amt as string)))
 ) as c32017,
collect_set(if(month_ious_normal_spreads_amt is null or month_ious_normal_spreads_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32018'), cast(month_ious_normal_spreads_amt as string)))
 ) as c32018,
collect_set(if(month_ious_overdue_count is null or month_ious_overdue_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32019'), cast(month_ious_overdue_count as string)))
 ) as c32019,
collect_set(if(month_ious_overdue_prcp_amt is null or month_ious_overdue_prcp_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32020'), cast(month_ious_overdue_prcp_amt as string)))
 ) as c32020,
collect_set(if(month_ious_overdue_int_amt is null or month_ious_overdue_int_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32021'), cast(month_ious_overdue_int_amt as string)))
 ) as c32021,
collect_set(if(month_ious_overdue_fee_amt is null or month_ious_overdue_fee_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32022'), cast(month_ious_overdue_fee_amt as string)))
 ) as c32022,
collect_set(if(month_ious_overdue_fine_amt is null or month_ious_overdue_fine_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32023'), cast(month_ious_overdue_fine_amt as string)))
 ) as c32023,
collect_set(if(month_ious_overdue_spreads_amt is null or month_ious_overdue_spreads_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32024'), cast(month_ious_overdue_spreads_amt as string)))
 ) as c32024,
collect_set(if(month_ious_tiqian_count is null or month_ious_tiqian_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32025'), cast(month_ious_tiqian_count as string)))
 ) as c32025,
collect_set(if(month_ious_tiqian_prcp_amt is null or month_ious_tiqian_prcp_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32026'), cast(month_ious_tiqian_prcp_amt as string)))
 ) as c32026,
collect_set(if(month_ious_tiqian_int_amt is null or month_ious_tiqian_int_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32027'), cast(month_ious_tiqian_int_amt as string)))
 ) as c32027,
collect_set(if(month_ious_tiqian_fee_amt is null or month_ious_tiqian_fee_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32028'), cast(month_ious_tiqian_fee_amt as string)))
 ) as c32028,
collect_set(if(month_ious_tiqian_fine_amt is null or month_ious_tiqian_fine_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32029'), cast(month_ious_tiqian_fine_amt as string)))
 ) as c32029,
collect_set(if(month_ious_tiqian_spreads_amt is null or month_ious_tiqian_spreads_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '32030'), cast(month_ious_tiqian_spreads_amt as string)))
 ) as c32030,
collect_set(if(month_ious_error_G00004_count is null or month_ious_error_G00004_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32031'), cast(month_ious_error_G00004_count as string)))
 ) as c32031,
collect_set(if(month_ious_daytime_repay_count is null or month_ious_daytime_repay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32032'), cast(month_ious_daytime_repay_count as string)))
 ) as c32032,
collect_set(if(month_ious_night_repay_count is null or month_ious_night_repay_count = 0, null,
 concat_ws('=', concat_ws('_', month, '32033'), cast(month_ious_night_repay_count as string)))
 ) as c32033

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.ious_repay_uid_month
WHERE is_not_null(user_id)
) t17
ON t.user_id = t17.user_id

WHERE
    t17.month >= t.begin_month and t17.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

-- Step 5.3: 拿去花-退款

CREATE VIEW tsfm.ious_refund_middle as

SELECT TRANSFORM(*)
USING 'python tsfm_combine_string_to_json.py'
as (user_id string, end_month string, features_json string)
FROM (
SELECT t.user_id, t.end_month,

collect_set(if(month_ious_total_refund_count is null or month_ious_total_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33001'), cast(month_ious_total_refund_count as string)))
 ) as c33001,
collect_set(if(month_ious_total_serv_refund_amt is null or month_ious_total_serv_refund_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '33002'), cast(month_ious_total_serv_refund_amt as string)))
 ) as c33002,
collect_set(if(month_ious_total_balance_refund_amt is null or month_ious_total_balance_refund_amt = 0, null,
 concat_ws('=', concat_ws('_', month, '33003'), cast(month_ious_total_balance_refund_amt as string)))
 ) as c33003,
collect_set(if(month_ious_pay_refund_count is null or month_ious_pay_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33004'), cast(month_ious_pay_refund_count as string)))
 ) as c33004,
collect_set(if(month_ious_preauth_refund_count is null or month_ious_preauth_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33005'), cast(month_ious_preauth_refund_count as string)))
 ) as c33005,
collect_set(if(month_ious_success_refund_count is null or month_ious_success_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33006'), cast(month_ious_success_refund_count as string)))
 ) as c33006,
collect_set(if(month_ious_fail_refund_count is null or month_ious_fail_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33007'), cast(month_ious_fail_refund_count as string)))
 ) as c33007,
collect_set(if(month_ious_process_refund_count is null or month_ious_process_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33008'), cast(month_ious_process_refund_count as string)))
 ) as c33008,
collect_set(if(month_ious_error_100063_count is null or month_ious_error_100063_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33009'), cast(month_ious_error_100063_count as string)))
 ) as c33009,
collect_set(if(month_ious_error_G00004_count is null or month_ious_error_G00004_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33010'), cast(month_ious_error_G00004_count as string)))
 ) as c33010,
collect_set(if(month_ious_error_G00008_count is null or month_ious_error_G00008_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33011'), cast(month_ious_error_G00008_count as string)))
 ) as c33011,
collect_set(if(month_ious_error_QF500010_count is null or month_ious_error_QF500010_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33012'), cast(month_ious_error_QF500010_count as string)))
 ) as c33012,
collect_set(if(month_ious_nonotice_count is null or month_ious_nonotice_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33013'), cast(month_ious_nonotice_count as string)))
 ) as c33013,
collect_set(if(month_ious_waitnotice_count is null or month_ious_waitnotice_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33014'), cast(month_ious_waitnotice_count as string)))
 ) as c33014,
collect_set(if(month_ious_successnotice_count is null or month_ious_successnotice_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33015'), cast(month_ious_successnotice_count as string)))
 ) as c33015,
collect_set(if(month_ious_failnotice_count is null or month_ious_failnotice_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33016'), cast(month_ious_failnotice_count as string)))
 ) as c33016,
collect_set(if(month_ious_daytime_refund_count is null or month_ious_daytime_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33017'), cast(month_ious_daytime_refund_count as string)))
 ) as c33017,
collect_set(if(month_ious_night_refund_count is null or month_ious_night_refund_count = 0, null,
 concat_ws('=', concat_ws('_', month, '33018'), cast(month_ious_night_refund_count as string)))
 ) as c33018

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN

(
SELECT *
FROM
feature_transform.ious_refund_uid_month
WHERE is_not_null(user_id)
) t18
ON t.user_id = t18.user_id

WHERE
    t18.month >= t.begin_month and t18.month < t.end_month

GROUP BY t.user_id, t.end_month
) mid;

---------------------------------------------------------------------------------------------------

-- Step Final: 合并用户所有的图

CREATE TABLE ${hiveconf:target_table} as

SELECT 
transform(t.user_id, t.begin_month, t.end_month,
          mid1.features_json,
          mid2.features_json,
          mid3.features_json,
          mid4.features_json,
          mid5.features_json,
          mid6.features_json,
          mid7.features_json,
          mid8.features_json,
          mid9.features_json,
          mid10.features_json,
          mid11.features_json,
          mid12.features_json,
          mid13.features_json,
          mid14.features_json,
          mid15.features_json,
          mid16.features_json,
          mid17.features_json,
          mid18.features_json)
USING 'python tsfm_combine_json_to_json.py'
as (user_id string, begin_month string, end_month string, features_json string)

FROM ${hiveconf:source_table} t

LEFT OUTER JOIN
tsfm.order_general_features_middle mid1
ON t.user_id = mid1.user_id and t.end_month = mid1.end_month

LEFT OUTER JOIN
tsfm.order_flight_features_middle mid2
ON t.user_id = mid2.user_id and t.end_month = mid2.end_month

LEFT OUTER JOIN
tsfm.order_hotel_features_middle mid3
ON t.user_id = mid3.user_id and t.end_month = mid3.end_month

LEFT OUTER JOIN
tsfm.order_train_features_middle mid4
ON t.user_id = mid4.user_id and t.end_month = mid4.end_month

LEFT OUTER JOIN
tsfm.order_others_features_middle mid5
ON t.user_id = mid5.user_id and t.end_month = mid5.end_month

LEFT OUTER JOIN
tsfm.pay_features_middle mid6
ON t.user_id = mid6.user_id and t.end_month = mid6.end_month

LEFT OUTER JOIN
tsfm.bankcard_features_middle mid7
ON t.user_id = mid7.user_id and t.end_month = mid7.end_month

LEFT OUTER JOIN
tsfm.contact_features_middle mid8
ON t.user_id = mid8.user_id and t.end_month = mid8.end_month

LEFT OUTER JOIN
tsfm.log_user_features_middle mid9
ON t.user_id = mid9.user_id and t.end_month = mid9.end_month

LEFT OUTER JOIN
tsfm.log_flight_features_middle mid10
ON t.user_id = mid10.user_id and t.end_month = mid10.end_month

LEFT OUTER JOIN
tsfm.log_train_features_middle mid11
ON t.user_id = mid11.user_id and t.end_month = mid11.end_month

LEFT OUTER JOIN
tsfm.log_hotel_features_middle mid12
ON t.user_id = mid12.user_id and t.end_month = mid12.end_month

LEFT OUTER JOIN
tsfm.log_group_features_middle mid13
ON t.user_id = mid13.user_id and t.end_month = mid13.end_month

LEFT OUTER JOIN
tsfm.log_other_features_middle mid14
ON t.user_id = mid14.user_id and t.end_month = mid14.end_month

LEFT OUTER JOIN
tsfm.log_unknown_features_middle mid15
ON t.user_id = mid15.user_id and t.end_month = mid15.end_month

LEFT OUTER JOIN
tsfm.ious_loan_middle mid16
ON t.user_id = mid16.user_id and t.end_month = mid16.end_month

LEFT OUTER JOIN
tsfm.ious_repay_middle mid17
ON t.user_id = mid17.user_id and t.end_month = mid17.end_month

LEFT OUTER JOIN
tsfm.ious_refund_middle mid18
ON t.user_id = mid18.user_id and t.end_month = mid18.end_month;

-- 订单
DROP VIEW if exists tsfm.order_general_features_middle;
DROP VIEW if exists tsfm.order_flight_features_middle;
DROP VIEW if exists tsfm.order_hotel_features_middle;
DROP VIEW if exists tsfm.order_train_features_middle;
DROP VIEW if exists tsfm.order_others_features_middle;

-- 支付 卡库 常旅
DROP VIEW if exists tsfm.pay_features_middle;
DROP VIEW if exists tsfm.bankcard_features_middle;
DROP VIEW if exists tsfm.contact_features_middle;

-- 拿去花
DROP VIEW if exists tsfm.ious_loan_middle;
DROP VIEW if exists tsfm.ious_repay_middle;
DROP VIEW if exists tsfm.ious_refund_middle;

-- 日志
DROP VIEW if exists tsfm.log_user_features_middle;
DROP VIEW if exists tsfm.log_flight_features_middle;
DROP VIEW if exists tsfm.log_train_features_middle;
DROP VIEW if exists tsfm.log_hotel_features_middle;
DROP VIEW if exists tsfm.log_group_features_middle;
DROP VIEW if exists tsfm.log_other_features_middle;
DROP VIEW if exists tsfm.log_unknown_features_middle;

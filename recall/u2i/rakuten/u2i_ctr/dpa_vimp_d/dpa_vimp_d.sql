DROP TABLE IF EXISTS z_yukimura.dpa_rakuten_ad_result;

CREATE TABLE IF NOT EXISTS z_yukimura.dpa_rakuten_ad_result WITH (
    format = 'ORC',
    partitioned_by = ARRAY ['type', 'publisher_id','dt']
) AS with item_data as (
    select ots,
        creative_id,
        mc_item_id
    from hive_ad.default.action_vimp
    where dt >= date_format(date_add('day', -4, date('2020-08-05')), '%Y-%m-%d')
),
campaign_creative_dpa as (
    select creative_id,
    merchandise_catalog_id as core_label
    from rds_ad.smartad.campaign_creative
    where merchandise_catalog_id in (70006, 70007, 70012, 70015, 70016, 70017)
),
dpa_sample_last_3_day as (
    SELECT ots,
        mc_item_id,
        item_data.creative_id as creative_id,
        core_label,
        date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d') as dt
    FROM item_data
        join campaign_creative_dpa on item_data.creative_id = campaign_creative_dpa.creative_id
),
ad_result as (
    SELECT *
    FROM ml.ad_result_v3
    WHERE dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d')
),
dpa_ad_result as (
    SELECT mc_item_id,
    core_label,
        t1.*
    FROM ad_result t1
        JOIN dpa_sample_last_3_day t2 on t1.ots = t2.ots
)
SELECT *
FROM dpa_ad_result;


delete from z_yukimura.dpa_rakuten_ad_result where dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d');
insert into z_yukimura.dpa_rakuten_ad_result with item_data as (
    select ots,
        creative_id,
        mc_item_id
    from hive_ad.default.action_vimp
    where dt >= date_format(date_add('day', -4, date('2020-08-05')), '%Y-%m-%d')
),
campaign_creative_dpa as (
    select creative_id,
    merchandise_catalog_id as core_label
    from rds_ad.smartad.campaign_creative
    where merchandise_catalog_id in (70006, 70007, 70012, 70015, 70016, 70017)
),
dpa_sample_last_3_day as (
    SELECT ots,
        mc_item_id,
        item_data.creative_id as creative_id,
        core_label
    FROM item_data
        join campaign_creative_dpa on item_data.creative_id = campaign_creative_dpa.creative_id
),
ad_result as (
    SELECT *
    FROM ml.ad_result_v3
    WHERE dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d')
),
dpa_ad_result as (
    SELECT mc_item_id,
    core_label,
        t1.*
    FROM ad_result t1
        JOIN dpa_sample_last_3_day t2 on t1.ots = t2.ots
)
SELECT *
FROM dpa_ad_result;


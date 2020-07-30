/*
 冷启动：
 用户：
 1. 10天内有广告行为，但是没有用户行为的用户数量，dpa效果
 2. 10天内内广告行为数量<100个曝光的用户数量，dpa效果
 商品：
 1. 10天内用户行为小于1000个的item的效果
 2. 10天内用户人数小于1000个的item的效果
 */
with ad_result AS (
    SELECT dt,
        ots,
        ad_id,
        creative_id,
        sales_e6 / 1e6 as sales,
        postback AS pb,
        click
    FROM hive_ad.ml.ad_result_v3
    WHERE dt > date_format(date_add('day', -10, now()), '%Y-%m-%d')
        AND dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
),
ad_result_ext AS (
    select ots,
        least(social_welfare, 1E5) as sw
    FROM hive_ad.common.ad_result_ext
    WHERE dt > date_format(date_add('day', -10, now()), '%Y-%m-%d')
        AND dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
),
campaign_creative AS (
    SELECT creative_id,
        merchandise_catalog_id
    FROM rds_ad.smartad.campaign_creative
    WHERE merchandise_catalog_id in (70006, 70007, 70015, 70016)
),
user_behavior AS (
    select ad_id,
        regexp_replace(
            replace(content_id, ' '),
            '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$',
            '$2:$3'
        ) as content_id,
        replace(behavior_types, 'adjust:callback:', '') as behavior_types,
        dt
    from (
            select split(
                    j(
                        element_at(atp.data, 'partnerparameters') [1],
                        '$.content_id'
                    ),
                    ','
                ) as content_ids,
                element_at(atp.data, 'adid') [1] as ad_id,
                element_at(atp.data, 'event') [1] as behavior_types,
                cast(element_at(atp.data, 'createdat') [1] as bigint) as ts,
                dt
            from hive_ad.default.ad_pixel atp
            where atp.dt > date_format(date_add('day', -10, now()), '%Y-%m-%d')
                and atp.dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
                and element_at(atp.data, 'storeid') [1] in ('jp.co.rakuten.android', '419267350')
                and j(
                    element_at(atp.data, 'partnerparameters') [1],
                    '$.content_id'
                ) is not null
                and atp.log_type in (
                    'adjust:callback:ViewContent',
                    'adjust:callback:AddToCart',
                    'adjust:callback:revenue'
                )
        )
        cross join UNNEST(content_ids) as t(content_id)
    where ad_id not in (
            '',
            '00000000-0000-0000-0000-000000000000',
            'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
            'DEFACE00-0000-0000-0000-000000000000'
        )
),
user_ad_cold_start as (
    SELECT ad_id,
        COUNT(*) as vimp,
        MAX(1) as ad_cold_start
    FROM ad_result
    WHERE dt <= date_format(date_add('day', -4, now()), '%Y-%m-%d')
    GROUP BY 1
    HAVING COUNT(*) < 100
),
user_behavior_cold_start as (
    select ad_id,
        COUNT(behavior_types) as behavior_count,
        MAX(1) as behavior_cold_start
    from user_behavior
    where dt <= date_format(date_add('day', -4, now()), '%Y-%m-%d')
    GROUP BY 1
    HAVING COUNT(behavior_types) < 10
),
ad_result_in_evalutation_perio AS (
    SELECT ad_id,
        a.ots,
        sales,
        creative_id,
        sw,
        pb,
        click
    FROM ad_result a
        join ad_result_ext b on a.ots = b.ots
    WHERE dt > date_format(date_add('day', -4, now()), '%Y-%m-%d')
),
user_ad_in_evalutation_perio AS (
    SELECT a.ad_id as ad_id,
        sales,
        sw,
        pb,
        ad_cold_start as ad_cold_start,
        behavior_cold_start as behavior_cold_start,
        click
    FROM ad_result_in_evalutation_perio a
        left join user_ad_cold_start b on a.ad_id = b.ad_id
        left join user_behavior_cold_start c on a.ad_id = c.ad_id
        join campaign_creative d on a.creative_id = d.creative_id
)
select date_format(date_add('day', -4, now()), '%Y-%m-%d') as dt,
    CASE
        WHEN ad_cold_start = 1 THEN 'ad_cold_start'
        WHEN behavior_cold_start = 1 THEN 'behavior_cold_start'
        ELSE 'without_cold_start'
    END as label,
    COUNT(distinct ad_id) as cold_start_user_count,
    SUM(sales) as sales,
    SUM(sw) as sw,
    COUNT(*) as vimp,
    SUM(click) as click,
    SUM(pb) as pb,
    COUNT(
        DISTINCT (
            CASE
                WHEN ad_cold_start = 1 THEN ad_id
            END
        )
    ) AS ad_cold_start_user_count,
    COUNT(
        DISTINCT (
            CASE
                WHEN behavior_cold_start = 1 THEN ad_id
            END
        )
    ) AS behavior_cold_start_user_count
FROM user_ad_in_evalutation_perio
GROUP BY 1, 2
ORDER BY 1, 2;
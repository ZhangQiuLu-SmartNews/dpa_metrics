/*
    active rakuten unique item (named Item Act) count impressed in SmartNews along with campaign
    users impressed by the Item Act, count their user behavior in Rakuten with Item Act
*/
with ad_result as (
    select ots,
        ad_id,
        dt,
        creative_id,
        uuid,
        sales_e6,
        postback,
        click
    from hive_ad.ml.ad_result_v3
    WHERE dt > date_format(date_add('day', -8, now()), '%Y-%m-%d')
        and dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
        and ad_id not in (
            '',
            '00000000-0000-0000-0000-000000000000',
            'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
            'DEFACE00-0000-0000-0000-000000000000'
        )
),
item_data as (
    select ots,
        dt,
        mc_item_id
    from hive_ad.default.action_vimp
    where dt > date_format(date_add('day', -8, now()), '%Y-%m-%d')
        and dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
),
ad_result_ext as (
    select ots,
        dt,
        social_welfare
    from common.ad_result_ext
    where dt > date_format(date_add('day', -8, now()), '%Y-%m-%d')
        and dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
),
campaign_creative as (
    select creative_id
    from rds_ad.smartad.campaign_creative
    where merchandise_catalog_id in (70006, 70007, 70015, 70016)
),
user_behavior as (
    select ad_id,
        regexp_replace(
            replace(content_id, ' '),
            '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$',
            '$2:$3'
        ) as content_id,
        dt,
        replace(behavior_types, 'adjust:callback:', '') as behavior_types
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
            where atp.dt > date_format(date_add('day', -8, now()), '%Y-%m-%d')
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
    group by 1,
        2,
        3,
        4
)
select ad_result.dt as dt,
    count(distinct mc_item_id) as unique_active_item_count,
    count(distinct content_id) as unique_behavior_item_count,
    sum(
        if(behavior_types in ('ViewContent'), 1, 0)
    ) as active_ui_view_count,
    sum(
        if(behavior_types in ('AddToCart'), 1, 0)
    ) as active_ui_add_to_car_count,
    sum(
        if(behavior_types in ('revenue'), 1, 0)
    ) as active_ui_revenue_count,
    SUM(sales_e6 / 1e6) AS sales,
    count(ad_result.ots) AS vimp,
    SUM(postback) AS pb,
    SUM(click) As click,
    ROUND(SUM(click) * 1.0 / COUNT(*), 5) AS vctr,
    ROUND(SUM(postback) * 1.0 / SUM(click), 5) AS cvr,
    ROUND(SUM(postback) * 1.0 / COUNT(*), 5) AS vctcvr,
    ROUND(SUM(sales_e6) / 1e6 * 1000 / COUNT(*)) AS vrpm,
    ROUND(SUM(sales_e6 / 1e6) / SUM(postback)) AS cpa
from ad_result
    join item_data on ad_result.ots = item_data.ots
    join ad_result_ext on ad_result.ots = ad_result_ext.ots
    join campaign_creative on ad_result.creative_id = campaign_creative.creative_id
    left join user_behavior on ad_result.ad_id = user_behavior.ad_id
    and user_behavior.content_id = item_data.mc_item_id
    and ad_result.dt = user_behavior.dt
group by 1
order by 1;
/*
 SmartNews user in 30 day
 Rakuten user behavior in 7 day
 total events, unique_users_count, unique_items_count, total_view_count, total_add_to_car_count, total_revenue_count
 */
with user_behavior as (
    select ad_id,
        regexp_replace(
            replace(content_id, ' '),
            '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$',
            '$2:$3'
        ) as content_id,
        replace(behavior_types, 'adjust:callback:', '') as behavior_types,
        ts
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
                cast(element_at(atp.data, 'createdat') [1] as bigint) as ts
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
),
user_adv as (
    select distinct ad_id
    from hive_ad.ml.ad_result_v3 v
    JOIN rds_ad.smartad.campaign_creative cc ON cc.creative_id = v.creative_id
    where v.dt > date_format(date_add('day', -30, now()), '%Y-%m-%d')
        and v.dt <= date_format(now(), '%Y-%m-%d')
        and cc.merchandise_catalog_id in (70006, 70007, 70015, 70016)
)
select count(*) as total_events,
    count(distinct ad_id) as unique_users_count,
    count(distinct content_id) as unique_items_count,
    sum(
        if(behavior_types in ('ViewContent'), 1, 0)
    ) as total_view_count,
    sum(
        if(behavior_types in ('AddToCart'), 1, 0)
    ) as total_add_to_car_count,
    sum(
        if(behavior_types in ('revenue'), 1, 0)
    ) as total_revenue_count
from (
        select u.ad_id as ad_id,
            u.content_id as content_id,
            u.behavior_types as behavior_types,
        from user_behavior u
            inner join user_adv ad on u.ad_id = ad.ad_id
    );
delete from z_yukimura.dpa_rakuten_user_item_behavior_d where dt = date_format(date_add('day', -1, date('${dt}')), '%Y-%m-%d')
;

-- next --

sleep 5;

-- next --

insert into z_yukimura.dpa_rakuten_user_item_behavior_d with user_behavior AS (
        select ad_id,
            regexp_replace(
                replace(content_id, ' '),
                '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$',
                '$2:$3'
            ) as content_id,
            CASE
                WHEN replace(behavior_types, 'adjust:callback:', '') = 'ViewContent' THEN 1
                WHEN replace(behavior_types, 'adjust:callback:', '') = 'AddToCart' THEN 2
                WHEN replace(behavior_types, 'adjust:callback:', '') = 'revenue' THEN 3
            END as behavior_type,
            dt,
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
                    cast(element_at(atp.data, 'createdat') [1] as bigint) as ts,
                    dt
                from hive_ad.default.ad_pixel atp
                where atp.dt >= date_format(
                        date_add('day', -1, date('${dt}')),
                        '%Y-%m-%d'
                    )
                    and atp.dt <= date_format(
                        date_add('day', -1, date('${dt}')),
                        '%Y-%m-%d'
                    )
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
    )
SELECT ad_id,
    content_id as item_id,
    behavior_type,
    ts,
    dt
FROM user_behavior
where content_id not in ('', ' ')
;
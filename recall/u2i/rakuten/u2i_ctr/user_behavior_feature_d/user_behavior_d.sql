-- set CURRENT_DATE='2020-08-05';

delete from z_yukimura.dpa_rakuten_user_behavior where dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d')
;

insert into z_yukimura.dpa_rakuten_user_behavior with user_behavior AS (
        select ad_id,
            regexp_replace(
                replace(content_id, ' '),
                '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$',
                '$2:$3'
            ) as content_id,
            replace(behavior_types, 'adjust:callback:', '') as behavior_type,
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
                where atp.dt >= date_format(date_add('day', -7, date('2020-08-05')), '%Y-%m-%d')
                    and atp.dt <= date_format(date_add('day', -2, date('2020-08-05')), '%Y-%m-%d')
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
    user_behavior_agg_lag_7 as (
        SELECT ad_id,
            date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d') as dt,
            array_agg(
                content_id
                order by ts desc
            ) as content_ids,
            array_agg(
                behavior_type
                order by ts desc
            ) as behavior_types
        FROM user_behavior
        WHERE content_id is not NULL
            and content_id != ''
        GROUP BY ad_id
    )
SELECT ad_id,
    content_ids,
    behavior_types,
    dt
FROM user_behavior_agg_lag_7;
DROP TABLE IF EXISTS z_yukimura.cluster_user_item_behavior;
CREATE TABLE IF NOT EXISTS z_yukimura.cluster_user_item_behavior (
    ad_id VARCHAR,
    item_seq ARRAY<VARCHAR>,
    category_seq ARRAY<VARCHAR>,
    dt VARCHAR
) WITH (
    format = 'TextFile',
    partitioned_by = ARRAY ['dt']
);

delete from z_yukimura.cluster_user_item_behavior where dt='2020-08-01'

insert into z_yukimura.cluster_user_item_behavior
with data as (
    select
        ad_id,
        regexp_replace(replace(content_id, ' '), '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3') as content_id,
        split(regexp_replace(replace(content_id, ' '), '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3'), ':')[1] as categoru_id,
        ts
    from (
        select
            split(j(element_at(atp.data, 'partnerparameters')[1], '$.content_id'), ',') as content_ids,
            element_at(atp.data, 'adid')[1] as ad_id,
            element_at(atp.data, 'event')[1] as behavior_types,
            cast(element_at(atp.data, 'createdat')[1] as bigint) as ts
        from hive_ad.default.ad_pixel atp
        where atp.dt > date_format(date_add('day', -1, date('2020-08-01')),'%Y-%m-%d')
            and atp.dt <= date_format(date_add('day', 0, date('2020-08-01')),'%Y-%m-%d')
            and element_at(atp.data, 'storeid')[1] in ('jp.co.rakuten.android', '419267350')
            and j(element_at(atp.data, 'partnerparameters')[1], '$.content_id') is not null
            and atp.log_type in (
                                'adjust:callback:ViewContent',
                                'adjust:callback:AddToCart',
                                'adjust:callback:revenue')
    )
    cross join UNNEST(content_ids) as t(content_id)
),
user_behavior as (
    select
        ad_id,
        array_agg(content_id order by ts) as item_seq,
        array_agg(categoru_id order by ts) as category_seq,
        '2020-08-01' as dt
    from data
    where ad_id not in ('',
                       '00000000-0000-0000-0000-000000000000',
                       'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                       'DEFACE00-0000-0000-0000-000000000000')
    group by 1
)
select *
from user_behavior
;
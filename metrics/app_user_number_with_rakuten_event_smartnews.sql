with adv_user as ( 
    select 
        distinct ad_id
    from (
        select 
            split(j(element_at(atp.data, 'partnerparameters')[1], '$.content_id'), ',') as content_ids,  # inventory_item_id
            element_at(atp.data, 'adid')[1] as ad_id,  # app_device_ad_id
            element_at(atp.data, 'event')[1] as behavior_types,  # outside behavior
            cast(element_at(atp.data, 'createdat')[1] as bigint) as ts
        from hive_ad.default.ad_pixel atp
        where atp.dt > date_format(date_add('day', -2, now()), '%Y-%m-%d')
            and atp.dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
            and element_at(atp.data, 'storeid')[1] in ('jp.co.rakuten.android', '419267350')
            and j(element_at(atp.data, 'partnerparameters')[1], '$.content_id') is not null
            and atp.log_type in ('adjust:callback:ViewContent',
                                'adjust:callback:AddToCart',
                                'adjust:callback:revenue')
    )
    where ad_id not in ('',
                        '00000000-0000-0000-0000-000000000000',
                        'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                        'DEFACE00-0000-0000-0000-000000000000')
)
select 
    count(distinct adv.ad_id)
from hive_ad.ml.ad_result_v3 v
inner join adv_user adv 
    on adv.ad_id = v.ad_id
        and v.dt > date_format(date_add('day', -30, now()), '%Y-%m-%d')
        and v.dt <= date_format(now(), '%Y-%m-%d');

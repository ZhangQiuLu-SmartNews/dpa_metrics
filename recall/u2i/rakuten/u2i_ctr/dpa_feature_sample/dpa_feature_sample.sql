delete from z_yukimura.dpa_rakuten_ad_feature_sample
where dt = date_format(
        date_add('day', -1, date('2020-08-05')),
        '%Y-%m-%d'
    );
INSERT INTO z_yukimura.dpa_rakuten_ad_feature_sample with dpa_rakuten_ad_result as (
        SELECT ots,
            ad_id_plus,
            mc_item_id,
            core_label
        FROM z_yukimura.dpa_rakuten_ad_result
        where dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d')
        and ad_id_plus = '0021AD26-0A6C-440A-B945-2852670FE2BE'
    ),
    feature_material as (
        SELECT ots,
            ad_id_plus,
            campaign_id,
            cf,
            cxf,
            uf,
            click AS ctr_label,
            postback AS cvr_label,
            dt
        FROM ml.feature_material
        WHERE dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d')
        and ad_id_plus = '0021AD26-0A6C-440A-B945-2852670FE2BE'
    ),
    dpa_rakuten_user_behavior_feature as (
        SELECT ad_id,
            user_behavior_f
        FROM z_yukimura.dpa_rakuten_user_behavior_feature
        WHERE dt = date_format(date_add('day', -1, date('2020-08-05')), '%Y-%m-%d')
        and ad_id = '0021AD26-0A6C-440A-B945-2852670FE2BE'
    )
SELECT t1.ots as ots,
    t1.ad_id_plus as ad_id_plus,
    campaign_id,
    mc_item_id as item_id,
    cast(core_label as VARCHAR) as core_label,
    user_behavior_f as itemf,
    cf,
    cxf,
    uf,
    ctr_label,
    cvr_label,
    dt
FROM dpa_rakuten_ad_result t1
    LEFT JOIN feature_material t2 ON t1.ots = t2.ots
    LEFT JOIN dpa_rakuten_user_behavior_feature t3 ON t1.ad_id_plus = t3.ad_id
    AND if(ctr_label > 0, true, rand(7) <= 0.1);
USE z_yukimura;
-- DROP TABLE IF EXISTS z_yukimura.dpa_rakuten_ad_feature_sample;
CREATE EXTERNAL TABLE IF NOT EXISTS z_yukimura.dpa_rakuten_ad_feature_sample (
    ots STRING COMMENT 'ots, user_ad_time level day t',
    ad_id_plus STRING COMMENT 'ad_id_plus',
    campaign_id STRING COMMENT 'campaign id',
    item_id STRING COMMENT 'inventory item id',
    core_label STRING COMMENT 'inventory core label',
    itemf STRING COMMENT 'inventory item feature day t-1',
    cf STRING COMMENT 'campaign feature: realtime + daily',
    cxf STRING COMMENT 'context feature',
    uf STRING COMMENT 'user feature: realtime + daily',
    ctr_label DOUBLE COMMENT 'ctr label',
    cvr_label DOUBLE COMMENT 'cvr label'
) PARTITIONED BY (dt STRING COMMENT 'date time') STORED AS ORC location 's3://smartad-dmp/warehouse/user/yukimura/dpa_rakuten_ad_feature_sample';

INSERT OVERWRITE TABLE z_yukimura.dpa_rakuten_ad_feature_sample partition (dt = '2020-08-04')
SELECT t1.ots as ots,
    t1.ad_id_plus as ad_id_plus,
    campaign_id,
    mc_item_id as item_id,
    core_label,
    user_behavior_f as itemf,
    cf,
    cxf,
    uf,
    ctr_label,
    cvr_label
FROM (
        SELECT ots,
            ad_id_plus,
            mc_item_id,
            core_label
        FROM z_yukimura.dpa_rakuten_ad_result
        where dt = date_sub('2020-08-05', 1)
    ) AS t1
LEFT JOIN (
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
        WHERE dt = date_sub('2020-08-05', 1)
    ) AS t2 ON t1.ots = t2.ots
LEFT JOIN (
    SELECT
        ad_id,
        user_behavior_f
    FROM z_yukimura.dpa_rakuten_user_behavior_feature
    WHERE dt = date_sub('2020-08-05', 1)
) AS t3 ON t1.ad_id_plus = t3.ad_id
WHERE if(ctr_label > 0, true, rand(7) <= 0.2)
;
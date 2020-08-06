
ADD JAR s3://smartad-dmp/lib/sn-feature-hive-latest.jar;
ADD JAR s3://smartad-dmp/lib/brickhouse-extended-latest-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION str_to_map AS 'brickhouse.udf.collect.GenericUDFStringToMap';
CREATE TEMPORARY FUNCTION add_prefix AS 'brickhouse.udf.collect.CollectionAddPrefixUDF';
CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
CREATE TEMPORARY FUNCTION merge_json AS 'brickhouse.udf.json.MergeJsonUDF';
CREATE TEMPORARY FUNCTION feature_transform as 'com.smartnews.feature.hive.udf.GenericUDFFeatureTransform';

-- DROP TABLE IF EXISTS z_yukimura.dpa_rakuten_ad_feature_transform;
CREATE EXTERNAL TABLE IF NOT EXISTS z_yukimura.dpa_rakuten_ad_feature_transform (
    ots STRING COMMENT 'ots, user_ad_time level day t',
    ad_id_plus STRING COMMENT 'ad_id_plus',
    campaign_id STRING COMMENT 'campaign id',
    item_id STRING COMMENT 'inventory item id',
    core_label STRING COMMENT 'inventory core label',
    features STRING COMMENT 'feature day t-1',
    ctr_label DOUBLE COMMENT 'ctr label',
    cvr_label DOUBLE COMMENT 'cvr label'
) PARTITIONED BY (dt STRING COMMENT 'date time') STORED AS ORC location 's3://smartad-dmp/warehouse/user/yukimura/dpa_rakuten_ad_feature_transform';

INSERT OVERWRITE TABLE z_yukimura.dpa_rakuten_ad_feature_transform partition (dt = '2020-08-04')
SELECT
    ots,
    ad_id_plus,
    campaign_id,
    item_id,
    core_label,
    feature_transform(if(itemf is not NULL, merge_json(itemf, uf), uf), cf, cxf, 's3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/conf/u2i_dnn_feature_transformation_config.json') AS features,
    ctr_label,
    cvr_label
FROM z_yukimura.dpa_rakuten_ad_feature_sample
WHERE dt = date_sub('2020-08-05', 1)
and itemf is not NULL and uf is not Null
limit 100;
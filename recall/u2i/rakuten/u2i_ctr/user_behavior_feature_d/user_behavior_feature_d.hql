ADD JAR s3://smartad-dmp/lib/brickhouse-extended-latest-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION str_to_map AS 'brickhouse.udf.collect.GenericUDFStringToMap';
CREATE TEMPORARY FUNCTION add_prefix AS 'brickhouse.udf.collect.CollectionAddPrefixUDF';
CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
CREATE TEMPORARY FUNCTION merge_json AS 'brickhouse.udf.json.MergeJsonUDF';
DROP TABLE IF EXISTS z_yukimura.tmp_dpa_rakuten_user_behavior_feature;
CREATE TABLE z_yukimura.tmp_dpa_rakuten_user_behavior_feature AS
SELECT
    ad_id,
    content_ids                                                                     AS content_ids_list,
    behavior_types                                                                  AS behavior_types_list,
    IF(array_contains(behavior_types, 'ViewContent'), 1, 0)                         AS has_view,
    IF(array_contains(behavior_types, 'AddToCart'), 1, 0)                           AS has_add_to_car,
    dt
FROM z_yukimura.dpa_rakuten_user_behavior
WHERE dt = date_sub('2020-08-05', 1)
;

CREATE EXTERNAL TABLE IF NOT EXISTS z_yukimura.dpa_rakuten_user_behavior_feature (
    ad_id STRING COMMENT 'ad_id',
    user_behavior_f STRING COMMENT 'user_behavior_f'
)
PARTITIONED BY (dt STRING COMMENT 'date time')
STORED AS ORC
location 's3://smartad-dmp/warehouse/user/yukimura/dpa_rakuten_user_behavior_feature';

INSERT OVERWRITE TABLE z_yukimura.dpa_rakuten_user_behavior_feature partition (dt = '2020-08-04')
SELECT
    ad_id,
    to_json(named_struct(
            'content_ids_list',                                             content_ids_list,
            'behavior_types_list',                                          behavior_types_list,
            'has_view',                                                     has_view,
            'has_add_to_car',                                               has_add_to_car
        )
    ) AS user_behavior_f
FROM z_yukimura.tmp_dpa_rakuten_user_behavior_feature
WHERE dt = date_sub('2020-08-05', 1)
;

DROP TABLE IF EXISTS z_yukimura.tmp_dpa_rakuten_user_behavior_feature;
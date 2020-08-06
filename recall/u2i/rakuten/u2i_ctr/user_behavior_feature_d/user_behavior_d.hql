-- drop table if exists z_yukimura.dpa_rakuten_user_behavior;

create table if not exists z_yukimura.dpa_rakuten_user_behavior (
    ad_id STRING,
    content_ids array<STRING>,
    behavior_types array<STRING>
) partitioned by (dt STRING)
stored as rcfile
location 's3://smartad-dmp/warehouse/user/yukimura/dpa_rakuten_user_behavior'
;

ADD ARCHIVE s3://smartad-dmp/ml/dnn/tf_record/tf_env.tar.gz;
ADD FILE s3://smartad-dmp/ml/dnn/tf_record/tf_record_env.sh;

ADD FILE s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/conf/tf_record_udf.py;
ADD FILE s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/conf/u2i_dnn_feature_transformation_config.json;
ADD FILE s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/conf/u2i_dnn_feature_config.json;

SET hive.execution.engine=mr;
SET mapred.reduce.tasks=10;

SELECT transform(dt, features, ctr_label, cvr_label) USING 'tf_record_env.sh'
    AS (dt string, feature_meta string)
FROM (
         SELECT dt, features, ctr_label, cvr_label
         FROM z_yukimura.dpa_rakuten_ad_feature_transform
         WHERE dt = date_sub('2020-08-05', 1)
         DISTRIBUTE BY (round(rand(1) * 999))
    ) t1
DISTRIBUTE BY 1
;
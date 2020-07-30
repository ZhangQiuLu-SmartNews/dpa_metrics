SELECT v.dt as dt,
    count(distinct uuid) as uu,
    SUM(sales_e6 / 1e6) AS sales,
    count(v.ots) AS vimp,
    SUM(postback) AS pb,
    SUM(click) As click,
    ROUND(SUM(click) * 1.0 / COUNT(*), 5) AS vctr,
    ROUND(SUM(postback) * 1.0 / SUM(click), 5) AS cvr,
    ROUND(SUM(postback) * 1.0 / COUNT(*), 5) AS vctcvr,
    ROUND(SUM(sales_e6) / 1e6 * 1000 / COUNT(*)) AS vrpm,
    ROUND(SUM(sales_e6 / 1e6) / SUM(postback)) AS cpa,
    SUM(least(social_welfare, 1E5)) as sw
FROM hive_ad.ml.ad_result_v3 v
    inner join hive_ad.common.ad_result_ext ext on v.ots = ext.ots
    JOIN rds_ad.smartad.campaign_creative cc ON cc.creative_id = v.creative_id
WHERE v.dt > date_format(date_add('day', -2, now()), '%Y-%m-%d')
    and v.dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
    and cc.merchandise_catalog_id in (70006, 70007, 70015, 70016)
GROUP BY 1
ORDER BY 1
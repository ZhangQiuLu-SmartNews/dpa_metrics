/*
 user count with normal ad
 user count with rakuten dpa
 */
with ad_result as (
    SELECT dt,
        ots,
        creative_id,
        ad_id,
        sales_e6 / 1e6 as sales,
        postback AS pb,
        click
    FROM hive_ad.ml.ad_result_v3
    WHERE dt > date_format(date_add('day', -3, now()), '%Y-%m-%d')
        AND dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
),
ad_result_ext as (
    select ots,
        least(social_welfare, 1E5) as sw
    FROM hive_ad.common.ad_result_ext
    WHERE dt > date_format(date_add('day', -3, now()), '%Y-%m-%d')
        AND dt <= date_format(date_add('day', -1, now()), '%Y-%m-%d')
),
campaign_creative AS (
    SELECT creative_id,
        merchandise_catalog_id
    FROM rds_ad.smartad.campaign_creative
)
SELECT dt AS dt,
    COUNT(DISTINCT ad_id) AS active_user_count,
    SUM(sales) AS sales,
    COUNT(ots) AS vimp,
    SUM(pb) AS pb,
    SUM(click) As click,
    SUM(sw) as sw,
    COUNT(DISTINCT (CASE WHEN merchandise_catalog_id IS NOT NULL THEN ad_id END)) AS active_dpa_user_count,
    SUM(
        if(
            merchandise_catalog_id is not null,
            sales,
            0
        )
    ) AS sales_dpa,
    SUM(
        if(
            merchandise_catalog_id is not null,
            1,
            0
        )
    ) AS vimp_dpa,
    SUM(
        if(
            merchandise_catalog_id is not null,
            pb,
            0
        )
    ) AS pb_dpa,
    SUM(
        if(
            merchandise_catalog_id is not null,
            click,
            0
        )
    ) As click_dpa,
    SUM(
        if(
            merchandise_catalog_id is not null,
            sw,
            0
        )
    ) As sw_dpa,
    COUNT(DISTINCT (CASE WHEN merchandise_catalog_id in (70006, 70007, 70015, 70016) THEN ad_id END)) AS active_rakuten_user_count,
    SUM(
        if(
            merchandise_catalog_id in (70006, 70007, 70015, 70016),
            sales,
            0
        )
    ) AS sales_rakuten,
    SUM(
        if(
            merchandise_catalog_id in (70006, 70007, 70015, 70016),
            1,
            0
        )
    ) AS vimp_rakuten,
    SUM(
        if(
            merchandise_catalog_id in (70006, 70007, 70015, 70016),
            pb,
            0
        )
    ) AS pb_rakuten,
    SUM(
        if(
            merchandise_catalog_id in (70006, 70007, 70015, 70016),
            click,
            0
        )
    ) As click_rakuten,
    SUM(
        if(
            merchandise_catalog_id in (70006, 70007, 70015, 70016),
            sw,
            0
        )
    ) As sw_rakuten
FROM (
        select dt,
            ad_result_ext.ots as ots,
            ad_id,
            sales,
            pb,
            click,
            merchandise_catalog_id,
            sw
        FROM ad_result
            JOIN ad_result_ext on ad_result.ots = ad_result_ext.ots
            left JOIN campaign_creative ON ad_result.creative_id = campaign_creative.creative_id
    ) as joined_table
GROUP BY 1
ORDER BY 1;

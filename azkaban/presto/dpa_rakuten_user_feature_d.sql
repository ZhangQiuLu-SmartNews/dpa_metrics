delete from z_yukimura.dpa_rakuten_user_feature_d where dt = date_format(date_add('day', -1, date('${dt}')), '%Y-%m-%d');

-- next --

sleep 5;

-- next --

INSERT INTO z_yukimura.dpa_rakuten_user_feature_d WITH user_item_behavior_joined AS (
        SELECT ad_id,
            item_no,
            behavior_type,
            ts,
            date_format(
                date_add('day', -1, date('${dt}')),
                '%Y-%m-%d'
            ) AS dt
        FROM z_yukimura.dpa_rakuten_user_item_behavior_d t1
            JOIN z_yukimura.dpa_rakuten_item_active_d t2 on t1.item_id = t2.item_id
        WHERE t1.dt >= date_format(
                date_add('day', -3, date('${dt}')),
                '%Y-%m-%d'
            )
            AND t1.dt <= date_format(
                date_add('day', -1, date('${dt}')),
                '%Y-%m-%d'
            )
            AND t2.dt = date_format(
                date_add('day', -1, date('${dt_item}')),
                '%Y-%m-%d'
            )
    ),
    user_session_agg_lag_3 AS (
        SELECT ad_id,
            array_agg(
                item_no
                ORDER BY ts ASC
            ) AS item_seq,
            array_agg(
                behavior_type
                ORDER BY ts ASC
            ) AS behavior_seq,
            dt
        FROM user_item_behavior_joined
        WHERE item_no IS NOT NULL
        GROUP BY ad_id,
            dt
    ),
    user_session_car_agg_lag_3 AS (
        SELECT ad_id,
            array_agg(
                item_no
                ORDER BY ts ASC
            ) AS item_car,
            dt
        FROM user_item_behavior_joined
        WHERE item_no IS NOT NULL
            AND behavior_type = 2
        GROUP BY ad_id,
            dt
    ),
    user_session_purchase_agg_lag_3 AS (
        SELECT ad_id,
            array_agg(
                item_no
                ORDER BY ts ASC
            ) AS item_purchase,
            dt
        FROM user_item_behavior_joined
        WHERE item_no IS NOT NULL
            AND behavior_type = 3
        GROUP BY ad_id,
            dt
    ),
    user_feature_table AS (
        SELECT t1.ad_id,
            array_join(item_seq, ',') as item_seq,
            array_join(behavior_seq, ',') as behavior_seq,
            array_join(item_car, ',') as item_car,
            array_join(item_purchase, ',') as item_purchase,
            NULL as item_category,
            t1.dt as dt
        FROM user_session_agg_lag_3 t1
            LEFT JOIN user_session_car_agg_lag_3 t2 on t1.ad_id = t2.ad_id
            LEFT JOIN user_session_purchase_agg_lag_3 t3 on t1.ad_id = t3.ad_id
    )
SELECT *
FROM user_feature_table
;
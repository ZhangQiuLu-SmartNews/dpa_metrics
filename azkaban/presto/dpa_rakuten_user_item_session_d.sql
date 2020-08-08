delete from z_yukimura.dpa_rakuten_user_item_session_d where dt=date_format(date_add('day', -1, date('${dt_data}')), '%Y-%m-%d');

-- next --

sleep 5;

-- next --

INSERT INTO z_yukimura.dpa_rakuten_user_item_session_d WITH user_item_behavior_joined AS (
    SELECT ad_id,
        item_no,
        behavior_type,
        ts,
        t1.dt
    FROM z_yukimura.dpa_rakuten_user_item_behavior_d t1
        JOIN z_yukimura.dpa_rakuten_item_active_d t2 ON t1.item_id = t2.item_id
    WHERE t1.dt = date_format(
            date_add('day', -1, date('${dt_data}')),
            '%Y-%m-%d'
        )
    AND t2.dt = date_format(
            date_add('day', -1, date('${dt}')),
            '%Y-%m-%d'
        )
),
user_session_agg_lag_1 AS (
    SELECT ad_id,
        array_agg(
            item_no
            ORDER BY ts DESC
        ) AS item_nos,
        array_agg(
            behavior_type
            ORDER BY ts DESC
        ) AS behavior_types,
        dt
    FROM user_item_behavior_joined
    WHERE item_no IS NOT NULL
    GROUP BY ad_id,
        dt
)
SELECT *
FROM user_session_agg_lag_1
;

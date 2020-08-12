-- notice
-- now current day is T
-- the ${hiveconf:dt_data} means data dt in (T-N,...,T-1)
-- the ${hiveconf:dt} means day T-1
INSERT OVERWRITE TABLE z_yukimura.dpa_u2i_rakten_two_tower_train_sample_d partition (dt = '${hiveconf:dt_data}')
SELECT
    t1.ad_id,
    item_nos,
    behavior_types,
    item_no AS item_target,
    behavior_type AS label
FROM (
    SELECT ad_id,
            item_no,
            behavior_type,
            ts,
            t1.dt
        FROM z_yukimura.dpa_rakuten_user_item_behavior_d t1
            JOIN z_yukimura.dpa_rakuten_item_active_d t2 on t1.item_id = t2.item_id
        WHERE t1.dt = '${hiveconf:dt_data}'
            and t2.dt = '${hiveconf:dt}'
    ) t1
JOIN z_yukimura.dpa_rakuten_user_item_session_d t2
ON t1.ad_id = t2.ad_id
    AND t1.dt = date_add(t2.dt, 1)
WHERE SIZE(item_nos) >= 5
    AND behavior_type IN (2, 3);
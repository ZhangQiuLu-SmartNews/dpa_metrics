delete from z_yukimura.dpa_u2i_rakten_two_tower_train_sample_d where dt = date_format(date_add('day', -1, date('${dt}')), '%Y-%m-%d');

-- next --

sleep 5;

-- next --

INSERT INTO z_yukimura.dpa_u2i_rakten_two_tower_train_sample_d WITH user_item_behavior_joined AS (
        SELECT ad_id,
            item_no,
            behavior_type,
            ts,
            t1.dt
        FROM z_yukimura.dpa_rakuten_user_item_behavior_d t1
            JOIN z_yukimura.dpa_rakuten_item_d t2 on t1.item_id = t2.item_id
        WHERE t1.dt = date_format(
                date_add('day', -1, date('${dt}')),
                '%Y-%m-%d'
            )
    ),
    train_data_t_item_t_1_session AS (
        SELECT t1.ad_id,
            item_nos,
            behavior_types,
            item_no AS item_target,
            behavior_type AS label,
            t1.dt as dt
        FROM user_item_behavior_joined t1
            JOIN z_yukimura.dpa_rakuten_user_item_session_d t2 ON t1.ad_id = t2.ad_id
            AND t1.dt = date_format(
                date_add('day', 1, date(t2.dt)),
                '%Y-%m-%d'
            )
        WHERE cardinality(item_nos) >= 5
            and behavior_type in (2, 3)
    )
SELECT *
FROM train_data_t_item_t_1_session
;
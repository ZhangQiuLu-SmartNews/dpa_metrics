delete from z_yukimura.dpa_rakuten_item_d where dt = date_format(date_add('day', -1, date('${dt}')), '%Y-%m-%d')
;
-- next --

sleep 5;

-- next --

INSERT INTO z_yukimura.dpa_rakuten_item_d WITH distinct_item_list AS (
        SELECT distinct item_id
        FROM z_yukimura.dpa_rakuten_user_item_behavior_d
        WHERE dt >= date_format(
                date_add('day', -10, date('${dt}')),
                '%Y-%m-%d'
            )
            and dt <= date_format(
                date_add('day', -1, date('${dt}')),
                '%Y-%m-%d'
            )
        order by item_id
    ),
    max_pre_featue AS (
        SELECT 1 AS flag,
            MAX(item_no) + 1 as max_pre_item_no
        FROM z_yukimura.dpa_rakuten_item_d
        WHERE dt = date_format(
                date_add('day', -2, date('${dt}')),
                '%Y-%m-%d'
            )
    ),
    pre_rank AS (
        SELECT *
        FROM z_yukimura.dpa_rakuten_item_d
        WHERE dt = date_format(
                date_add('day', -2, date('${dt}')),
                '%Y-%m-%d'
            )
    ),
    outer_join AS (
        SELECT COALESCE(t1.item_id, t2.item_id) as item_id,
            t1.item_no AS item_no,
            1 AS flag
        FROM pre_rank t1
            FULL OUTER JOIN distinct_item_list t2 ON t1.item_id = t2.item_id
    ),
    left_join AS (
        SELECT item_id,
            COALESCE(item_no, max_pre_item_no) as item_no,
            date_format(
                date_add('day', -1, date('${dt}')),
                '%Y-%m-%d'
            ) as dt
        FROM outer_join t1
            LEFT JOIN max_pre_featue t2 ON t1.flag = t2.flag
    )
SELECT item_id,
    ROW_NUMBER() over (
        ORDER BY item_no,
            item_id ASC
    ) as item_no,
    dt
FROM left_join
;

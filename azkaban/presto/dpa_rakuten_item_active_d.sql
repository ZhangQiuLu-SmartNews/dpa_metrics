DELETE FROM z_yukimura.dpa_rakuten_item_active_d WHERE dt = date_format(date_add('day', -1, date('${dt}')),'%Y-%m-%d')
;
-- next --

sleep 5;

-- next --
INSERT INTO z_yukimura.dpa_rakuten_item_active_d WITH user_item_list AS (
    SELECT item_id,
        count(item_id) AS item_num
    FROM z_yukimura.dpa_rakuten_user_item_behavior_d
    WHERE dt >= date_format(
            date_add('day', -10, date('${dt}')),
            '%Y-%m-%d'
        )
        and dt <= date_format(
            date_add('day', -1, date('${dt}')),
            '%Y-%m-%d'
        )
    GROUP BY item_id
    ORDER BY 2 DESC
)
SELECT *,
    ROW_NUMBER() OVER (
        ORDER BY item_num DESC
    ) AS item_no,
    date_format(
        date_add('day', -1, date('${dt}')),
        '%Y-%m-%d'
    ) AS dt
FROM user_item_list LIMIT 20000
;
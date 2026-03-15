--建 user_item_beh_log 表, 后续 T-mall 数据集中

CREATE TABLE user_item_beh_log
(
    item_id  STRING COMMENT ''
    ,user_id STRING COMMENT ''
    ,action  STRING COMMENT ''
    ,vtime   STRING COMMENT ''
)
COMMENT 'part_a, part_b, part_c'
LIFECYCLE 180;
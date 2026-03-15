--维表: 用户ID

create table if not exists user_id_number (
    user_id string,
    number bigint
)LIFECYCLE 180;

INSERT OVERWRITE TABLE user_id_number
select user_id, ROW_NUMBER() OVER(ORDER BY rnd) AS number
from (
    select user_id, RAND() as rnd
    from (
        select DISTINCT user_id
        from user_item_beh_log
    )t1
)t1
;
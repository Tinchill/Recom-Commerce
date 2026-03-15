--@exclude_input=recom_commerce.dw_user_item_click_log
--@exclude_input=recom_commerce.dw_user_item_collect_log
--@exclude_input=recom_commerce.dw_user_item_alipay_log

-- 用户-品牌的交互序列特征
-- 在过往30天(过往90天)的时间限定范围内, 从top-1w个发生过购买的品牌里
-- 挑选出 top-50 的 点击/收藏/购买的品牌

create TABLE if not exists user_click_brand_seq_feature (
    user_id string,
    brand_id_seq string
)PARTITIONED BY (ds STRING) LIFECYCLE 180;

insert OVERWRITE TABLE user_click_brand_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('b_',brand_id)) as brand_id_seq
from (
    select t2.user_id, t2.brand_id
    from (
        select brand_id
        from brand_top1w_alipay_dim
    )t1 join (
        SELECT user_id,brand_id
            , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time desc) AS number
        from (
            select user_id, brand_id, max(op_time) as op_time
            from dw_user_item_click_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
                and brand_id is not null
            group by user_id, brand_id
        )t1
    )t2 on t1.brand_id=t2.brand_id
    where number<=50
)t1 group by user_id
;

create TABLE if not exists user_clt_brand_seq_feature (
    user_id string,
    brand_id_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 180;

insert OVERWRITE TABLE user_clt_brand_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('b_',brand_id)) as brand_id_seq
from (
    select t2.user_id, t2.brand_id
    from (
        select brand_id
        from brand_top1w_alipay_dim
    )t1 join (
        select user_id, brand_id
            , ROW_NUMBER() OVER(PARTITION BY user_id  ORDER BY op_time desc) AS number
        from (
            select user_id, brand_id, max(op_time) as op_time
            from dw_user_item_collect_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
                and brand_id is not null
            group by user_id, brand_id
        )t1
    )t2 on t1.brand_id=t2.brand_id
    where number<=50
)t1 group by user_id
;

create TABLE if not exists user_pay_brand_seq_feature (
    user_id string,
    brand_id_seq string
)PARTITIONED BY (ds STRING) LIFECYCLE 180;

insert OVERWRITE TABLE user_pay_brand_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('b_',brand_id)) as brand_id_seq
from (
    select t2.user_id, t2.brand_id
    from (
        select brand_id
        from brand_top1w_alipay_dim
    )t1 join (
        select user_id, brand_id,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC ) AS number
        from (
            select user_id, brand_id, max(op_time) as op_time
            from dw_user_item_alipay_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
                and brand_id is not null
            group by user_id, brand_id
        )t1
    )t2 on t1.brand_id=t2.brand_id
    where number<=50
)t1 group by user_id
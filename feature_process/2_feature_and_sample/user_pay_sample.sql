--@exclude_input=recom_commerce.user_id_number
--@exclude_input=recom_commerce.dw_user_item_alipay_log

--基于品牌进行采样
--正样本 : 从数据集中的实验日期开始往后的7天内, 购买了对应品牌的用户
--负样本 : 从数据集中的实验日期开始往后的7天内, 没有购买对应品牌的用户

--SQL语句中出现
--)t1 left anti join (
--    select user_id, brand_id
--    from user_pay_sample_pos
--    where ds = '${bizdate}'
--)t2 on t1.neg_user_id=t2.user_id and t1.brand_id=t2.brand_id

--这里 anti join 是为了把随机选出的样本中的正样本去除, 以得到真正的负样本

create table if not exists user_pay_sample_pos (
    user_id string,
    brand_id string
)PARTITIONED By (ds STRING) LIFECYCLE 180;

INSERT OVERWRITE TABLE user_pay_sample_pos PARTITION(ds=${bizdate})
select t1.user_id, t1.brand_id
from (
    select user_id, brand_id
    from dw_user_item_alipay_log
    where ds > '${bizdate}' and ds <= to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),7, 'dd'),'yyyymmdd')
    group by user_id, brand_id
)t1 join (
    select brand_id
    from brand_top500_alipay_dim
)t2 on t1.brand_id=t2.brand_id
;

create table if not exists user_pay_sample (
    user_id string,
    brand_id string,
    label bigint
)PARTITIONED By (ds STRING) LIFECYCLE 180;

INSERT OVERWRITE TABLE user_pay_sample PARTITION(ds=${bizdate})
select t1.neg_user_id as user_id, t1.brand_id, 0 as label
from (
    select DISTINCT t1.brand_id, t2.user_id as neg_user_id
    from (
        select TRANS_ARRAY(2, ',', user_id, brand_id, rand_neg) as (user_id, brand_id, rand_neg)
        from (
            select user_id ,brand_id, concat(
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint)
            ) as rand_neg
            from user_pay_sample_pos
            where ds = '${bizdate}'
        )t1
    )t1 join (
        select user_id, number
        from user_id_number
    )t2 on t1.rand_neg=t2.number
)t1 left anti join (
    select user_id, brand_id
    from user_pay_sample_pos
    where ds = '${bizdate}'
)t2 on t1.neg_user_id=t2.user_id and t1.brand_id=t2.brand_id
union all
select user_id, brand_id, 1 as label
from user_pay_sample_pos
where ds = '${bizdate}'
;

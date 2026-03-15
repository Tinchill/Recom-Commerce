--维表:商品表
--将T-mall数据集中的数据通过阿里云的ODPS cmd上传到此表中

CREATE TABLE item_dim
(
    item_id    STRING COMMENT ''
    ,title     STRING COMMENT ''
    ,pict_url  STRING COMMENT ''
    ,category  STRING COMMENT ''
    ,brand_id  STRING COMMENT ''
    ,seller_id STRING COMMENT ''
)
COMMENT '数据集：product表，6个字段'
LIFECYCLE 180;
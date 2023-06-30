CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customername` string
  , `email` string
  , `phone` string
  , `birthday` string
  , `serialnumber` string
  , `registrationdate` bigint
  , `lastupdatedate` bigint
  , `sharewithresearchasofdate` bigint
  , `sharewithfriendsasofdate` bigint
  , `sharewithpublicasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'FALSE', 'mapping' = 'TRUE')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-thanhtv35/project/customers/landing/'
TBLPROPERTIES ('classification' = 'json');
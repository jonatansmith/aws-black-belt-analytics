-- Create external table in input CSV files. Replace the S3 path with your bucket name

CREATE EXTERNAL TABLE blackbelt_db.roads_landing_csv(
  op                   string
  ,state               string
  ,name_of_facility    string
  ,toll_id             string  
  ,operating_authority string
  ,from_location       string
  ,to_location         string
  ,miles               string
  ,rural_urban         string
  ,kind                string
  ,Authority_Source    string
  ,Fee_Type            string
  ,vehicle_fee         string
  ,Truck_fee           string
  ,record_id           string
  ,cdc_ingestion_timestamp timestamp
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ddkdevapplication-datapi-lakehouseblackbeltbucket-izumx08ylkll/lakehouse/roads_landing_csv/'
TBLPROPERTIES (
  'areColumnsQuoted'='false', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'typeOfData'='file');
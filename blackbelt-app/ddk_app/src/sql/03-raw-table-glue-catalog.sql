-- Create external table in input CSV files. Replace the S3 path with your bucket name

CREATE TABLE blackbelt_db.tolldata_raw(
  state               string
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

PARTITIONED BY (state, bucket(16,toll_id)) 
LOCATION 's3://ddkdevapplication-datapi-lakehouseblackbeltbucket-izumx08ylkll/lakehouse/tolldata_raw/' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_target_data_file_size_bytes'='536870912' 
)
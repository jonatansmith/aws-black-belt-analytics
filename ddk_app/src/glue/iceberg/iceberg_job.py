import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, max

from pyspark.conf import SparkConf

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_job_catalog_warehouse'])
conf = SparkConf()

## Please make sure to pass runtime argument --iceberg_job_catalog_warehouse with value as the S3 path 
conf.set("spark.sql.catalog.job_catalog.warehouse", args['iceberg_job_catalog_warehouse'])
conf.set("spark.sql.catalog.job_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.job_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.job_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
conf.set("spark.sql.iceberg.handle-timestamp-without-timezone","true")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

## Read Input Table
IncrementalInputDyF = glueContext.create_dynamic_frame.from_catalog(database = "blackbelt_db", table_name = "roads_landing_csv", transformation_ctx = "IncrementalInputDyF")
IncrementalInputDF_with_null = IncrementalInputDyF.toDF()
IncrementalInputDF_with_null.printSchema()
IncrementalInputDF = IncrementalInputDF_with_null.fillna({'op':''})

if not IncrementalInputDF.rdd.isEmpty():
    ## Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation 
    IDWindowDF = Window.partitionBy(IncrementalInputDF.record_id).orderBy(IncrementalInputDF.cdc_ingestion_timestamp).rangeBetween(-sys.maxsize, sys.maxsize)

    # Add new columns to capture first and last OP value and what is the latest timestamp
    inputDFWithTS= IncrementalInputDF.withColumn("max_op_date",max(IncrementalInputDF.cdc_ingestion_timestamp).over(IDWindowDF))
    
    # Filter out new records that are inserted, then select latest record from existing records and merge both to get deduplicated output 
    NewInsertsDF = inputDFWithTS.filter("cdc_ingestion_timestamp=max_op_date").filter("op='I'")
    UpdateDeleteDf = inputDFWithTS.filter("cdc_ingestion_timestamp=max_op_date").filter("op IN ('U','D')")
    finalInputDF = NewInsertsDF.unionAll(UpdateDeleteDf)

    # Register the deduplicated input as temporary table to use in Iceberg Spark SQL statements
    finalInputDF.createOrReplaceTempView("incremental_input_data")
    finalInputDF.show()
    
    ## Perform merge operation on incremental input data with MERGE INTO. This section of the code uses Spark SQL to showcase the expressive SQL approach of Iceberg to perform a Merge operation
    IcebergMergeOutputDF = spark.sql("""
    MERGE INTO job_catalog.blackbelt_db.tolldata_raw t
    USING (SELECT op, state, name_of_facility, toll_id, operating_authority, from_location, to_location, miles, rural_urban, kind, Authority_Source, Fee_Type, vehicle_fee, Truck_fee, record_id, to_timestamp(cdc_ingestion_timestamp) as cdc_ingestion_timestamp FROM incremental_input_data) s
    ON t.record_id = s.record_id
    WHEN MATCHED AND s.op = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET t.name_of_facility = s.name_of_facility , t.toll_id = s.toll_id , t.operating_authority = s.operating_authority , t.from_location = s.from_location , t.to_location = s.to_location , t.miles = s.miles , t.rural_urban = s.rural_urban , t.kind = s.kind , t.Authority_Source = s.Authority_Source , t.Fee_Type = s.Fee_Type , t.vehicle_fee = s.vehicle_fee , t.Truck_fee = s.Truck_fee , t.record_id = s.record_id , t.cdc_ingestion_timestamp = s.cdc_ingestion_timestamp 
    WHEN NOT MATCHED THEN INSERT (state, name_of_facility, toll_id, operating_authority, from_location, to_location, miles, rural_urban, kind, Authority_Source, Fee_Type, vehicle_fee, Truck_fee, record_id,  cdc_ingestion_timestamp) VALUES (s.state, s.name_of_facility, s.toll_id, s.operating_authority, s.from_location, s.to_location, s.miles, s.rural_urban, s.kind, s.Authority_Source, s.Fee_Type, s.vehicle_fee, s.Truck_fee, s.record_id, s.cdc_ingestion_timestamp)
    """)

    job.commit()
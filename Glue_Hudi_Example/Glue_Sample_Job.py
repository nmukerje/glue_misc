import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.session import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.functions import concat, col, lit
import random

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

## Common Hudi Constants
# General Constants
HUDI_FORMAT = "org.apache.hudi"
TABLE_NAME = "hoodie.table.name"
RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field"
PRECOMBINE_FIELD_OPT_KEY = "hoodie.datasource.write.precombine.field"
OPERATION_OPT_KEY = "hoodie.datasource.write.operation"
BULK_INSERT_OPERATION_OPT_VAL = "bulk_insert"
UPSERT_OPERATION_OPT_VAL = "upsert"
BULK_INSERT_PARALLELISM = "hoodie.bulkinsert.shuffle.parallelism"
UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism"
S3_CONSISTENCY_CHECK = "hoodie.consistency.check.enabled"
HUDI_CLEANER_POLICY = "hoodie.cleaner.policy"
KEEP_LATEST_COMMITS = "KEEP_LATEST_COMMITS"
HUDI_COMMITS_RETAINED = "hoodie.cleaner.commits.retained"
PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
EMPTY_PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"

# Hive Constants
HIVE_SYNC_ENABLED_OPT_KEY="hoodie.datasource.hive_sync.enable"
HIVE_PARTITION_FIELDS_OPT_KEY="hoodie.datasource.hive_sync.partition_fields"
HIVE_ASSUME_DATE_PARTITION_OPT_KEY="hoodie.datasource.hive_sync.assume_date_partitioning"
HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY="hoodie.datasource.hive_sync.partition_extractor_class"
HIVE_TABLE_OPT_KEY="hoodie.datasource.hive_sync.table"
HIVE_JDBC_SYNC="hoodie.datasource.hive_sync.use_jdbc"
HIVE_DATABASE_OPT_KEY="hoodie.datasource.hive_sync.database"

# Partition Constants
NONPARTITION_EXTRACTOR_CLASS_OPT_VAL="org.apache.hudi.hive.NonPartitionedExtractor"
MULIPART_KEYS_EXTRACTOR_CLASS_OPT_VAL="org.apache.hudi.hive.MultiPartKeysValueExtractor"
KEYGENERATOR_CLASS_OPT_KEY="hoodie.datasource.write.keygenerator.class"
NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL="org.apache.hudi.keygen.NonpartitionedKeyGenerator"
COMPLEX_KEYGENERATOR_CLASS_OPT_VAL="org.apache.hudi.ComplexKeyGenerator"
PARTITIONPATH_FIELD_OPT_KEY="hoodie.datasource.write.partitionpath.field"

job_name = args['JOB_NAME']
logger.info(f'*** START {job_name} ***')

## CHANGE ME ##
config = {
    "hudi_database":"demo1",
    "hudi_table_name": "example_glue_hudi_partitioned_table",
    "target": "s3://<bucket>/prefix/example_glue_hudi_partitioned_table",
    "primary_key": "id",
    "sort_key": "sk",
    "commits_to_retain": "2",
    "partition_keys" : "year,month"
}

## Generate some random Data
## For Real Use Cases, you would typically read data from a Catalog Table.
## Generates Data - we are adding year and month columns this time.
def get_json_data(start, count, increment=0):
    data = [{"id": i, "sk": i+increment, "txt": chr(65 + (i % 26)), "year" : "2019", "month": random.randint(1,12) } for i in range(start, start + count)]
    return data

# Creates the Dataframe
def create_json_df(spark, data):
    sc = spark.sparkContext
    return spark.read.json(sc.parallelize(data, 2))

df1 = create_json_df(spark, get_json_data(0, 4000000))
df1.printSchema()

## Add the Partition columns year and month.
hudiTablePartitionKey="partitionKey"
df1 = df1.withColumn(hudiTablePartitionKey,concat(lit("year="),col("year"),lit("/month="),col("month")))
df1.select(hudiTablePartitionKey).show(5)

# Bulk Insert the Hudi table
(df1.write.format(HUDI_FORMAT)
      .option(PRECOMBINE_FIELD_OPT_KEY, config["sort_key"])
      .option(RECORDKEY_FIELD_OPT_KEY, config["primary_key"])
      .option(TABLE_NAME, config['hudi_table_name'])
      .option(OPERATION_OPT_KEY, BULK_INSERT_OPERATION_OPT_VAL)
      .option(BULK_INSERT_PARALLELISM, 3)
      .option(HIVE_PARTITION_FIELDS_OPT_KEY, config["partition_keys"])
      .option(HIVE_DATABASE_OPT_KEY,config['hudi_database'])
      .option(HIVE_TABLE_OPT_KEY,config['hudi_table_name'])
      .option(HIVE_SYNC_ENABLED_OPT_KEY,"true")
      .option(HIVE_JDBC_SYNC,"false")
      .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,MULIPART_KEYS_EXTRACTOR_CLASS_OPT_VAL)
      .option(PARTITIONPATH_FIELD_OPT_KEY,"partitionKey")
      .mode("Overwrite")
      .save(config['target']))

logger.info(f'*** END {job_name} ***')
job.commit()

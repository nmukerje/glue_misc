# Miscellaneous Glue scripts

1. glue_addpartition_lambda.py - AWS Lambda function triggered by S3 file drops that adds partitions to a table in the Glue catalog

2. glue_batch_create_partition.py - Python function that can reads S3 locations and bulk creates partitions in a table in the Glue catalog. Think of this as MSCK REPAIR for custom partitions.

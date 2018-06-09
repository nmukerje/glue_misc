import boto3

catalogId='<account id>'
databaseName='<database name>'
tableName='<table name>'
# partition keys
keys=['year','month','day']
region='us-east-1'

glue=boto3.client(service_name='glue', region_name=region)

def lambda_handler(event, context):
    s3_key=event['Records'][0]['s3']['object']['key']
    print (s3_key)
    bucket=event['Records'][0]['s3']['bucket']['name']
    
    values=s3_key.split("/")
    prefix=values[0]
    # Discarding 1st value - prefix and last value - filename.
    values=values[1:-1]
    
    location='/'.join(values)
    t=glue.get_table(CatalogId=catalogId,DatabaseName=databaseName,Name=tableName)
    
    # read in storage descriptor from table
    storageDescriptor= t['Table']['StorageDescriptor']
    partitionInput={"StorageDescriptor": { "Location":"s3://"+bucket+'/'+prefix+'/'+location+"/" } ,"Values":values}
    print (partitionInput)
    storageDescriptor['Location']=partitionInput['StorageDescriptor']['Location']
    partitionInput['StorageDescriptor']=storageDescriptor
    
    try:
       p=glue.create_partition(CatalogId=catalogId,DatabaseName=databaseName,TableName=tableName,PartitionInput=partitionInput)
       print ('Partition Added')
    except glue.exceptions.AlreadyExistsException as e:
       print ('Partition already exists')
    return

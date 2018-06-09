region='us-east-1'

def add_partitions(bucket,prefix,catalogId,databaseName,tableName):
   glue=boto3.client(service_name='glue', region_name=region)
   s3 = boto3.client('s3')
   batch=100

   response = s3.list_objects(
      Bucket=bucket,
      Prefix=prefix
   )
   
   keys=['/'.join(rec['Key'].split('/')[1:-1]) for rec in response['Contents'] if rec['Size'] > 0 ]
   keys=list(set(keys))   

   k=glue.get_table(CatalogId=catalogId,DatabaseName=databaseName,Name=tableName)
   storageDescriptor= k['Table']['StorageDescriptor']

   for k in izip_longest(*[iter(keys)]*batch):
      k=[x for x in k if x is not None]
      print len(k) 
      partitionInput=[{"StorageDescriptor": { "Location":"s3://"+prefix+'/'+rec+"/" } ,"Values":rec.split('/')} for rec in k]
      for a in partitionInput:
         storageDescriptor['Location']=a['StorageDescriptor']['Location']
         a['StorageDescriptor']=storageDescriptor
    
      p=glue.batch_create_partition(CatalogId=catalogId,DatabaseName=databaseName,TableName=tableName,PartitionInputList=partitionInput)
      print p

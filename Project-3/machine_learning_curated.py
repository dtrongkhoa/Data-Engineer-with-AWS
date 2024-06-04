import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1717504062166 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1717504062166")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1717504096731 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1717504096731")

# Script generated for node SQL Query
SqlQuery1665 = '''
select s.serialnumber, s.sensorreadingtime, s.distancefromobject from step_trainer s
join acce a 
on a.timestamp = s.sensorreadingtime;
'''
SQLQuery_node1717506269943 = sparkSqlQuery(glueContext, query = SqlQuery1665, mapping = {"step_trainer":StepTrainerTrusted_node1717504096731, "acce":AccelerometerTrusted_node1717504062166}, transformation_ctx = "SQLQuery_node1717506269943")

# Script generated for node Amazon S3
AmazonS3_node1717504260491 = glueContext.getSink(path="s3://project3-stedi-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1717504260491")
AmazonS3_node1717504260491.setCatalogInfo(catalogDatabase="project3-stedi-db",catalogTableName="machine_learning_curated")
AmazonS3_node1717504260491.setFormat("json")
AmazonS3_node1717504260491.writeFrame(SQLQuery_node1717506269943)
job.commit()
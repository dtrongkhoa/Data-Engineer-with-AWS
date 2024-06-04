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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1717499446620 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1717499446620")

# Script generated for node Customer Curated
CustomerCurated_node1717499366016 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1717499366016")

# Script generated for node SQL Query
SqlQuery1443 = '''
select s.serialnumber,s.sensorReadingTime, s.distanceFromObject
from step_trainer s
join customer c on c.serialnumber = s.serialnumber;
'''
SQLQuery_node1717499591700 = sparkSqlQuery(glueContext, query = SqlQuery1443, mapping = {"step_trainer":StepTrainerLanding_node1717499446620, "customer":CustomerCurated_node1717499366016}, transformation_ctx = "SQLQuery_node1717499591700")

# Script generated for node Amazon S3
AmazonS3_node1717499779501 = glueContext.getSink(path="s3://project3-stedi-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1717499779501")
AmazonS3_node1717499779501.setCatalogInfo(catalogDatabase="project3-stedi-db",catalogTableName="step_trainer_trusted")
AmazonS3_node1717499779501.setFormat("json")
AmazonS3_node1717499779501.writeFrame(SQLQuery_node1717499591700)
job.commit()
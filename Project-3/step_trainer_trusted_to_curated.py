import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node17174027628802 = glueContext.create_dynamic_frame.from_catalog(
    database="project3-stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node17174027628802",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node17174037475302 = glueContext.create_dynamic_frame.from_catalog(
    database="project3-stedi-db",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node17174037475302",
)

# Script generated for node Join
Join_node17174037496142 = Join.apply(
    frame1=StepTrainerTrusted_node17174037475302,
    frame2=AccelerometerTrusted_node17174027628802,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node17174037496142",
)

# Script generated for node Drop Fields
DropFields_node17174037513214 = DropFields.apply(
    frame=Join_node17174037496142,
    paths=["user"],
    transformation_ctx="DropFields_node17174037513214",
)

# Script generated for node Amazon S3
AmazonS3_node17174037572136 = glueContext.getSink(path="s3://project3-stedi-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node17174037572136")
AmazonS3_node17174037572136.setCatalogInfo(catalogDatabase="project3-stedi-db",catalogTableName="machine_learning_curated")
AmazonS3_node17174037572136.setFormat("json")
AmazonS3_node17174037572136.writeFrame(DropFields_node17174037513214)
job.commit()
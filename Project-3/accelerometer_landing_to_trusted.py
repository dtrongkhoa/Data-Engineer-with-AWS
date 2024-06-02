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

# Script generated for node Amazon S3
AmazonS3_node1717315292735 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="customer_trusted", transformation_ctx="AmazonS3_node1717315292735")

# Script generated for node Amazon S3
AmazonS3_node1717315278837 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1717315278837")

# Script generated for node Join
Join_node1717315322239 = Join.apply(frame1=AmazonS3_node1717315278837, frame2=AmazonS3_node1717315292735, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1717315322239")

# Script generated for node Drop Fields
DropFields_node1717315420859 = DropFields.apply(frame=Join_node1717315322239, paths=["serialnumber", "sharewithpublicasofdate", "birthday", "registrationdate", "sharewithresearchasofdate", "customername", "sharewithfriendsasofdate", "email", "lastupdatedate", "phone"], transformation_ctx="DropFields_node1717315420859")

# Script generated for node Amazon S3
AmazonS3_node1717315352785 = glueContext.getSink(path="s3://project3-stedi-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1717315352785")
AmazonS3_node1717315352785.setCatalogInfo(catalogDatabase="project3-stedi-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1717315352785.setFormat("json")
AmazonS3_node1717315352785.writeFrame(DropFields_node1717315420859)
job.commit()
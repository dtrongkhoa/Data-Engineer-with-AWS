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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1717317416732 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project3-stedi-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1717317416732")

# Script generated for node Customer Curated
CustomerCurated_node1717317474875 = glueContext.create_dynamic_frame.from_catalog(database="project3-stedi-db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1717317474875")

# Script generated for node Join
Join_node1717317544003 = Join.apply(frame1=StepTrainerLanding_node1717317416732, frame2=CustomerCurated_node1717317474875, keys1=["serialNumber"], keys2=["serialnumber"], transformation_ctx="Join_node1717317544003")

# Script generated for node Drop Fields
DropFields_node1717317586438 = DropFields.apply(frame=Join_node1717317544003, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate",], transformation_ctx="DropFields_node1717317586438")

# Script generated for node Amazon S3
AmazonS3_node1717317628802 = glueContext.getSink(path="s3://project3-stedi-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1717317628802")
AmazonS3_node1717317628802.setCatalogInfo(catalogDatabase="project3-stedi-db",catalogTableName="step_trainer_trusted")
AmazonS3_node1717317628802.setFormat("json")
AmazonS3_node1717317628802.writeFrame(DropFields_node1717317586438)
job.commit()
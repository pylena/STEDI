import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accc
accc_node1750812213932 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="acceleromete_trusted", transformation_ctx="accc_node1750812213932")

# Script generated for node customer
customer_node1750812186231 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="customer_trusted", transformation_ctx="customer_node1750812186231")

# Script generated for node Join
Join_node1750812233159 = Join.apply(frame1=customer_node1750812186231, frame2=accc_node1750812213932, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1750812233159")

# Script generated for node Drop Fields
DropFields_node1750812287963 = DropFields.apply(frame=Join_node1750812233159, paths=["z", "user", "x", "y"], transformation_ctx="DropFields_node1750812287963")

# Script generated for node Drop Duplicates
DropDuplicates_node1750813280487 =  DynamicFrame.fromDF(DropFields_node1750812287963.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750813280487")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1750813280487, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750811772272", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750812361494 = glueContext.getSink(path="s3://linapr/customer/cb/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750812361494")
AmazonS3_node1750812361494.setCatalogInfo(catalogDatabase="stedi-pr",catalogTableName="customer_curetdd")
AmazonS3_node1750812361494.setFormat("json")
AmazonS3_node1750812361494.writeFrame(DropDuplicates_node1750813280487)
job.commit()
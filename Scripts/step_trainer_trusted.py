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

# Script generated for node step
step_node1750822576959 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="step_trainer_landing", transformation_ctx="step_node1750822576959")

# Script generated for node Cureted
Cureted_node1750822452576 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="customer_curetd", transformation_ctx="Cureted_node1750822452576")

# Script generated for node Join
Join_node1750822611844 = Join.apply(frame1=Cureted_node1750822452576, frame2=step_node1750822576959, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1750822611844")

# Script generated for node Drop Fields
DropFields_node1750822715501 = DropFields.apply(frame=Join_node1750822611844, paths=["`.customername`", "birthday", "sharewithpublicasofdate", "sharewithresearchasofdate", "registrationdate", "customername", "`.sharewithpublicasofdate`", "sharewithfriendsasofdate", "`.lastupdatedate`", "`.birthday`", "`.sharewithfriendsasofdate`", "phone", "`.sharewithresearchasofdate`", "lastupdatedate", "email", "`.registrationdate`", "serialnumber"], transformation_ctx="DropFields_node1750822715501")

# Script generated for node Drop Duplicates
DropDuplicates_node1750822830349 =  DynamicFrame.fromDF(DropFields_node1750822715501.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750822830349")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1750822830349, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750822441635", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1750822864345 = glueContext.getSink(path="s3://linapr/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1750822864345")
step_trainer_trusted_node1750822864345.setCatalogInfo(catalogDatabase="stedi-pr",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1750822864345.setFormat("json")
step_trainer_trusted_node1750822864345.writeFrame(DropDuplicates_node1750822830349)
job.commit()
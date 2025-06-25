import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node customer trusted
customertrusted_node1750808322272 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="customer_trusted", transformation_ctx="customertrusted_node1750808322272")

# Script generated for node acclumater landing
acclumaterlanding_node1750808290263 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="accelerometer_landing", transformation_ctx="acclumaterlanding_node1750808290263")

# Script generated for node Join
Join_node1750808353420 = Join.apply(frame1=customertrusted_node1750808322272, frame2=acclumaterlanding_node1750808290263, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1750808353420")

# Script generated for node Drop Fields
DropFields_node1750808383415 = DropFields.apply(frame=Join_node1750808353420, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "timestamp"], transformation_ctx="DropFields_node1750808383415")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1750808383415, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750808166583", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1750808439526 = glueContext.getSink(path="s3://linapr/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1750808439526")
accelerometertrusted_node1750808439526.setCatalogInfo(catalogDatabase="stedi-pr",catalogTableName="acceleromete_trusted")
accelerometertrusted_node1750808439526.setFormat("json")
accelerometertrusted_node1750808439526.writeFrame(DropFields_node1750808383415)
job.commit()
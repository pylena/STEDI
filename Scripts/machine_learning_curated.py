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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714819087793 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linepr/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1714819087794")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714819086269 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linepr/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1714819086269")

# Script generated for node SQL Query
SqlQuery445 = '''
select step_tr.*, acc.user, acc.x, acc.y, acc.z from step join acc on step_tr.sensorreadingtime=acc.timestamp;
'''
SQLQuery_node1714819092652 = sparkSqlQuery(glueContext, query = SqlQuery445, mapping = {"step_tr":StepTrainerTrusted_node1714819087793, "acc":AccelerometerTrusted_node1714819086269}, transformation_ctx = "SQLQuery_node1714819092656")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1714819140798 = glueContext.getSink(path="s3://linepr/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1714819140798")
MachineLearningCurated_node1714819140798.setCatalogInfo(catalogDatabase="stedi-pr",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1714819140798.setFormat("json")
MachineLearningCurated_node1714819140798.writeFrame(SQLQuery_node1714819092652)
job.commit()
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

# Script generated for node Cust Landing
CustLanding_node1715064562859 = glueContext.create_dynamic_frame.from_catalog(database="stedi-pr", table_name="customer_landing", transformation_ctx="CustLanding_node1715064562859")

# Script generated for node SQL Query
SqlQuery327 = '''
select * from customer where shareWithResearchAsOfDate <>0;

'''
SQLQuery_node1715065842470 = sparkSqlQuery(glueContext, query = SqlQuery327, mapping = {"cust":CustLanding_node1715064562859}, transformation_ctx = "SQLQuery_node1715065842470")

# Script generated for node customer trusted
custtrusted_node1715064612119 = glueContext.getSink(path="s3://linepr/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="custtrusted_node1715064612119")
custtrusted_node1715064612119.setCatalogInfo(catalogDatabase="stedi-pr",catalogTableName="customer_trusted")
custtrusted_node1715064612119.setFormat("json")
custtrusted_node1715064612119.writeFrame(SQLQuery_node1715065842470)
job.commit()
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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZone_node1",
)

# Script generated for node PrivacyFilter
SqlQuery2094 = """
select * from myDataSource
where shareWithResearchAsOfDate IS NOT NULL

"""
PrivacyFilter_node1697318506304 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2094,
    mapping={"myDataSource": CustomerLandingZone_node1},
    transformation_ctx="PrivacyFilter_node1697318506304",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node2 = glueContext.getSink(
    path="s3://stedi-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node2",
)
TrustedCustomerZone_node2.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node2.setFormat("json")
TrustedCustomerZone_node2.writeFrame(PrivacyFilter_node1697318506304)
job.commit()

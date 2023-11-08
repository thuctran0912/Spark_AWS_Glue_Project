import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1697903336844 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1697903336844",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1697903539823 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1697903539823",
)

# Script generated for node Join Customer
CustomerTrustedZone_node1697903336844DF = CustomerTrustedZone_node1697903336844.toDF()
AccelerometerTrusted_node1697903539823DF = AccelerometerTrusted_node1697903539823.toDF()
JoinCustomer_node1697903520549 = DynamicFrame.fromDF(
    CustomerTrustedZone_node1697903336844DF.join(
        AccelerometerTrusted_node1697903539823DF,
        (
            CustomerTrustedZone_node1697903336844DF["email"]
            == AccelerometerTrusted_node1697903539823DF["user"]
        ),
        "leftsemi",
    ),
    glueContext,
    "JoinCustomer_node1697903520549",
)

# Script generated for node Drop Fields
DropFields_node1697903990102 = DropFields.apply(
    frame=JoinCustomer_node1697903520549,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1697903990102",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1698157014378 = DynamicFrame.fromDF(
    DropFields_node1697903990102.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1698157014378",
)

# Script generated for node Customer Curated
CustomerCurated_node1697903884882 = glueContext.getSink(
    path="s3://stedi-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1697903884882",
)
CustomerCurated_node1697903884882.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1697903884882.setFormat("json")
CustomerCurated_node1697903884882.writeFrame(DropDuplicates_node1698157014378)
job.commit()

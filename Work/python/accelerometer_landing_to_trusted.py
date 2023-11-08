import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1697903336844 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1697903336844",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1697903539823 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1697903539823",
)

# Script generated for node Join Customer
JoinCustomer_node1697903520549 = Join.apply(
    frame1=AccelerometerLanding_node1697903336844,
    frame2=CustomerTrustedZone_node1697903539823,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1697903520549",
)

# Script generated for node Drop Fields
DropFields_node1697903990102 = DropFields.apply(
    frame=JoinCustomer_node1697903520549,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1697903990102",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1697903884882 = glueContext.getSink(
    path="s3://stedi-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1697903884882",
)
AccelerometerTrusted_node1697903884882.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1697903884882.setFormat("json")
AccelerometerTrusted_node1697903884882.writeFrame(DropFields_node1697903990102)
job.commit()

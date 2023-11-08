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

# Script generated for node Step_trainer Landing Zone
Step_trainerLandingZone_node1698160310896 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="Step_trainerLandingZone_node1698160310896",
    )
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1698160406748 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedZone_node1698160406748",
)

# Script generated for node Renamed keys for Step_trainer Trusted
RenamedkeysforStep_trainerTrusted_node1698160787991 = ApplyMapping.apply(
    frame=CustomerCuratedZone_node1698160406748,
    mappings=[
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "bigint", "phone", "long"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        (
            "shareWithResearchAsOfDate",
            "double",
            "right_shareWithResearchAsOfDate",
            "double",
        ),
        (
            "shareWithPublicAsOfDate",
            "double",
            "right_shareWithPublicAsOfDate",
            "double",
        ),
        (
            "shareWithFriendsAsOfDate",
            "double",
            "right_shareWithFriendsAsOfDate",
            "double",
        ),
    ],
    transformation_ctx="RenamedkeysforStep_trainerTrusted_node1698160787991",
)

# Script generated for node Step_trainer Trusted
Step_trainerTrusted_node1698157534333 = Join.apply(
    frame1=Step_trainerLandingZone_node1698160310896,
    frame2=RenamedkeysforStep_trainerTrusted_node1698160787991,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Step_trainerTrusted_node1698157534333",
)

# Script generated for node Drop Fields
DropFields_node1698157692640 = DropFields.apply(
    frame=Step_trainerTrusted_node1698157534333,
    paths=[
        "right_customerName",
        "right_email",
        "right_birthDay",
        "right_serialNumber",
        "right_shareWithResearchAsOfDate",
        "right_shareWithPublicAsOfDate",
        "right_shareWithFriendsAsOfDate",
        "phone",
        "registrationDate",
        "lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1698157692640",
)

# Script generated for node Step_trainer Trusted Zone
Step_trainerTrustedZone_node1698157779356 = glueContext.getSink(
    path="s3://stedi-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Step_trainerTrustedZone_node1698157779356",
)
Step_trainerTrustedZone_node1698157779356.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
Step_trainerTrustedZone_node1698157779356.setFormat("json")
Step_trainerTrustedZone_node1698157779356.writeFrame(DropFields_node1698157692640)
job.commit()

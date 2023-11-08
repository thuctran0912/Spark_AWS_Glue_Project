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

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1698163306329 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedZone_node1698163306329",
    )
)

# Script generated for node Step_trainer Trusted Zone
Step_trainerTrustedZone_node1698163411354 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house/step_trainer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="Step_trainerTrustedZone_node1698163411354",
    )
)

# Script generated for node Join
Join_node1698163465379 = Join.apply(
    frame1=AccelerometerTrustedZone_node1698163306329,
    frame2=Step_trainerTrustedZone_node1698163411354,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1698163465379",
)

# Script generated for node Drop Fields
DropFields_node1698163485762 = DropFields.apply(
    frame=Join_node1698163465379,
    paths=["sensorReadingTime"],
    transformation_ctx="DropFields_node1698163485762",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1698163497701 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698163485762,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1698163497701",
)

job.commit()

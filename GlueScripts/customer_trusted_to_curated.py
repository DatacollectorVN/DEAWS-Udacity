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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1645561118623 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1645561118623",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1683561120566 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1683561120566",
)

# Script generated for node Join
Join_node1645561202139 = Join.apply(
    frame1=AWSGlueDataCatalog_node1645561118623,
    frame2=AWSGlueDataCatalog_node1683561120566,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1645561202139",
)

# Script generated for node Drop Fields
DropFields_node1645561234982 = DropFields.apply(
    frame=Join_node1645561202139,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1645561234982",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1645561293964 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1645561234982,
    database="stedi",
    table_name="customer_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1645561293964",
)

job.commit()

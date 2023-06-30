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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1669402494231 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1669402494231",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Join Customer and Step Trainer
JoinCustomerandStepTrainer_node166935698367 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1669402494231,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="JoinCustomerandStepTrainer_node166935698367",
)

# Script generated for node Drop Fields
DropFields_node1669402760896 = DropFields.apply(
    frame=JoinCustomerandStepTrainer_node166935698367,
    paths=["user"],
    transformation_ctx="DropFields_node1669402760896",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1669578616430 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1669402760896,
    database="stedi",
    table_name="machine_learning_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="StepTrainerTrusted_node1669578616430",
)

job.commit()

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://input-csv-data-06-04-2025/")

df.show()

columns = [
    "alien_id", "alien_name", "species", "home_planet",
    "strength_level", "speed_level", "intelligence"
]

df = df.select([col(c).cast("string").alias(c) for c in columns])

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# Step 2: Write data to S3 as Parquet
output_path = "s3://output-parquet-data-06-04-2025/"
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx="datasink"
)

# Step 3: Update Glue table schema using boto3
glue_client = boto3.client('glue')

# Infer columns (basic type mapping)
columns = [{"Name": col, "Type": "string"} for col in df.columns]  # You can enhance this with type mapping

# Fetch current table to preserve storage settings
existing_table = glue_client.get_table(DatabaseName="jaladatax", Name="ben10")
print("Column names:")
for i, col in enumerate(df.columns):
    print(f"{i}: {repr(col)}")
    
# Update schema only
glue_client.update_table(
    DatabaseName="jaladatax",
    TableInput={
        'Name': 'ben10',
        'StorageDescriptor': {
            **existing_table['Table']['StorageDescriptor'],
            'Columns': columns,
            'Location': output_path  # Ensure the output path is accurate
        },
        'TableType': 'EXTERNAL_TABLE'
    }
)

job.commit()

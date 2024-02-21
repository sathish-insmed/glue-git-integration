import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
#from datetime import datetime
from pyspark.sql import SparkSession
import pandas as pd
from subprocess import call 
import boto3
from io import BytesIO

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Installing openpyxl package
def install(package):
    call([sys.executable,"-m","pip","install",package])
install("openpyxl")

#file path and file name
s3_bucket= "test-globalreporting"
s3_key="TEST_UPLOAD/Excel/TEST_EXCEL.xlsx"

# Initialize S3 client
s3_client = boto3.client('s3')

# Read the previous modified date from the BytesIO buffer or set an initial value
previous_modified_date_buffer = BytesIO()

try:
    # Read the previous modified date from the specified location
    s3_client.download_fileobj(s3_bucket, 'previous_modified_date',previous_modified_date_buffer)
    previous_modified_date_buffer.seek(0)
    previous_modified_date_str = previous_modified_date_buffer.read().decode('utf-8')
    previous_modified_date = datetime.strptime(previous_modified_date_str, "%Y-%m-%d %H:%M:%S.%f")
except FileNotFoundError:
    # Set an initial value if it's the first run
    previous_modified_date = datetime.min

# Get the last modified date of the Excel file
response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
last_modified_date = response['LastModified'].replace(tzinfo=None)

# Compare last modified date with the previous one
if last_modified_date > previous_modified_date:
    # File has been modified, proceed with reading the Excel file and performing necessary operations
    print("File has been modified. Reading Excel file...")

    # Your Glue job logic here...

    # Update the previous modified date to the current value in the BytesIO buffer
    previous_modified_date_buffer = BytesIO()
    previous_modified_date_buffer.write(last_modified_date.strftime("%Y-%m-%d %H:%M:%S.%f").encode('utf-8'))

    # Upload the updated previous modified date to the specified location
    previous_modified_date_buffer.seek(0)
    s3_client.upload_fileobj(previous_modified_date_buffer, s3_bucket, 'path/to/previous_modified_date.txt')

else:
    print("File has not been modified. Exiting.")

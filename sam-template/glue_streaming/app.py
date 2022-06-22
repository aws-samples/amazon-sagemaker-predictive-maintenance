#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#


# Import libraries, set up job parameters and variables
import sys
import datetime
import boto3
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'out_bucket_name', 'glue_table_name', 'glue_database_name']) 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 output locations
out_bucket_name = args['out_bucket_name']
checkpoint_location = "s3://" + out_bucket_name + "/cp/"

# Function that gets called to perform processing, feature engineering and writes to S3 for every micro batch of streaming data from Kinesis.
def processBatch(data_frame, batchId):
    transformer = pipeline.fit(data_frame)
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    if (data_frame.count() > 0):
        data_frame = transformer.transform(data_frame)
        data_frame = data_frame.drop("type")
        data_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        data_frame.printSchema()
        # Write output features to S3
        s3prefix = "features" + "/year=" + "{:0>4}".format(str(year)) + "/month=" + "{:0>2}".format(str(month)) + "/day=" + "{:0>2}".format(str(day)) + "/hour=" + "{:0>2}".format(str(hour)) + "/min=" + "{:0>2}".format(str(minute)) + "/batchid=" + str(batchId)
        s3path = "s3://" + out_bucket_name + "/" + s3prefix + "/"
        print("-------write start time------------")
        print(str(datetime.datetime.now()))
        data_frame = data_frame.toDF().repartition(1)
        data_frame.write.mode("overwrite").option("header",False).csv(s3path)
        print("-------write end time------------")
        print(str(datetime.datetime.now()))

# Read from Kinesis Data Stream
sourceStreamData = glueContext.create_data_frame.from_catalog(database = args['glue_database_name'], table_name = args['glue_table_name'], transformation_ctx = "sourceStreamData", additional_options = {"startingPosition": "TRIM_HORIZON"})
type_indexer = StringIndexer(inputCol="type", outputCol="type_enc", stringOrderType="alphabetAsc")
pipeline = Pipeline(stages=[type_indexer])
glueContext.forEachBatch(frame = sourceStreamData, batch_function = processBatch, options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})
job.commit()
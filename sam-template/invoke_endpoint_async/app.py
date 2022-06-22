#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')
sm_runtime = boto3.client('sagemaker-runtime')


def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        endpt_response = sm_runtime.invoke_endpoint_async(EndpointName="Predictive-Maintainance-XgBoost", InputLocation="s3://" + bucket + "/" + key , ContentType="csv")
        return endpt_response
    except Exception as e:
        print(e)
        #print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
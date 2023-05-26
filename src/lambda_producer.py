import os
import random
import json
from datetime import datetime

import boto3


def producer(event, context):
    """
    Scheduled CRON lambda function which produces example files,
    simulating some external data producer. Files are placed on
    a bucket which has a listner to trigger the `handle_new_s3_file`
    function to process them.
    """
    # We're only really needing to demonstrate 'some file' landing on
    # s3 which triggers some downstream processing. Trying not to convolute
    # the example with any added complexity, so we'll deposite a file which has
    # a number and downstream processing can decide how to interpret it.
    bucket = os.environ["S3_BUCKET"]

    d = datetime.utcnow()
    key = f"data/year={d.year}/month={d.month}/day={d.day}/hour={d.hour}/minute={d.minute}/second={d.second}/data.json"

    data = json.dumps({"count": random.randint(10, 1_000)}).encode()

    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)

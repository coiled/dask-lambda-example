import os
import random
import json
import functools
from datetime import datetime

import boto3
import coiled
from distributed import Client, wait

from dask_processing import process_s3_file


def connect_to_cluster(error_no_cluster=False):
    """
    A helper decorator to add a `Optional[distributed.Client]` parameter to functions.
    Passing a `distributed.Client` if a Dask cluster is already running, otherwise `None`
    """

    def inner(f):
        @functools.wraps(f)
        def wrapper(event, context):
            client = boto3.client("secretsmanager")
            resp = client.get_secret_value(SecretId="CLUSTER_METADATA")
            metadata = json.loads(resp["SecretString"])

            if "cluster-addr" in metadata:
                client = Client(metadata["cluster-addr"])
            elif error_no_cluster:
                raise RuntimeError("No running cluster found.")
            else:
                client = None

            return f(event, context, client)

        return wrapper


@connect_to_cluster(error_no_cluster=True)
def consumer(event, context, client):
    """
    Lambda function triggered on new S3 files which need processing.

    It connects and offloads the processing work to an existing cluster.
    This is _very_ helpful in ETL type jobs where the Lambda resources can
    remain consistent across different processing jobs/files because the Lambda
    function itself doesn't perform any heavy movement/compute work. It only
    coordinates the work to be done on an existing cluster.
    """
    print(event)

    # Get bucket and key of file triggering this function
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Offload the processing to the cluster
    job = client.submit(process_s3_file, bucket, key)
    wait(job)

    print(job.result())


@connect_to_cluster(error_no_cluster=False)
def start_stop_cluster(event, context, client):
    """
    Scheduled CRON Lambda function which starts a Dask cluster using
    Coiled, then stores connection information in SecretsManager
    """
    if event["action"] == "start":
        print(event)
        if client is not None:
            return  # Cluster already running

        date = datetime.utcnow()
        cluster = coiled.Cluster(
            name=f"processing-cluster-{date.year}-{date.month}-{date.day}",
        )
    elif event["action"] == "stop":
        if client is None:
            return  # No cluster
        client.shutdown()
    else:
        raise ValueError(f"Unknown action '{event['action']}'")

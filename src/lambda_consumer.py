import sys
import os
import random
import json
import functools
import subprocess
import hashlib
from datetime import datetime

import boto3
import coiled
from distributed import Client, wait, PipInstall

import dask_processing
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
            resp = client.get_secret_value(SecretId=os.environ["SECRET_ARN"])
            metadata = json.loads(resp["SecretString"])

            cluster_name = metadata.get("CLUSTER_NAME")
            if cluster_name:
                cluster = coiled.Cluster(
                    name=cluster_name, shutdown_on_close=False, credentials="local"
                )
                client = cluster.get_client()
            elif error_no_cluster:
                raise RuntimeError("No running cluster found.")
            else:
                client = None

            return f(event, context, client)

        return wrapper

    return inner

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
    key = event["Records"][0]["s3"]["object"]["key"].replace("%3D", "=")

    value = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    print(value)


    # Offload the processing to the cluster
    job = client.submit(process_s3_file, bucket, key)
    wait(job)

    return


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
            software=_software_environment(),
            shutdown_on_close=False,
            n_workers=4,
            worker_cpu=2,
        )
        client = cluster.get_client()

        # cluster created with software env due to needing superset of current env
        # and no 'mixed' package_sync w/ software env but it will need the processing module
        client.upload_file(dask_processing.__file__)
        _update_secret(cluster.get_client())
    elif event["action"] == "stop":
        if client is None:
            return  # No cluster
        client.cluster.shutdown()
        _update_secret()
    else:
        raise ValueError(f"Unknown action '{event['action']}'")


def _update_secret(client=None):
    boto3.client("secretsmanager").put_secret_value(
        SecretId=os.environ["SECRET_ARN"],
        SecretString=json.dumps(
            {}
            if client is None
            else {
                "CLUSTER_NAME": client.cluster.name,
                "SCHEDULER_ADDR": client.scheduler.address,
                "DASHBOARD_ADDR": client.dashboard_link,
            }
        ),
    )


def _current_environment():
    # cmd = sys.executable + " -m pip freeze"
    # return subprocess.check_output(cmd.split()).decode().splitlines()
    # This would 'ideally' work, but technically with layers they aren't
    # installed packages, only in the PYTHONPATH so aren't caught.
    # Instead (probably a better way?) we stored them in an env var since
    # there aren't many.
    return os.environ["INSTALLED_PKGS"].splitlines()


def _software_environment():
    # TODO: Software environment combined with package_sync
    # since we want a superset of current env
    deps = _current_environment()
    deps.extend(["dask[dataframe]", "s3fs", "bokeh==2.4.2"])
    env_hash = hashlib.md5("".join(deps).encode()).hexdigest()[:5]
    name = f"milesg-processing-cluster-{env_hash}"
    coiled.create_software_environment(name=name, pip=deps)
    return name

from datetime import datetime, timedelta
import json
import boto3
import pathlib


def process_s3_file(bucket, key):
    """
    Process a s3 file
    """
    import dask
    import dask.dataframe as dd

    # **NOTE** Import any external deps here, so the client
    # which runs on Lambda and may not have heavier libs,
    # doesn't get imported there.
    resp = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    count = json.loads(resp["Body"].read().decode())["count"]

    # Example processing:
    # We are just going to create some busy work by
    # making a timeseries in the span of 'count' and computing
    # the mean, then returning; something that would be very
    # uncomfortable to do on Lambda.

    # In practice, reading the file, massaging the data then writing
    # to S3, Redshift, etc would be more useful.

    end = datetime.utcnow()
    start = end - timedelta(days=count)
    timeseries = dask.datasets.timeseries(start, end)
    result = timeseries.groupby("name").mean().y.std().compute()
    return result

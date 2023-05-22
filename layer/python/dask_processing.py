from datetime import datetime, timedelta
import boto3
import dask
import dask.dataframe as dd


def process_s3_file(bucket, key):
    """
    Process a s3 file
    """
    # **NOTE** Import any external deps here, so the client
    # which runs on Lambda and may not have heavier libs,
    # doesn't get imported there.
    data = dd.read_json(f"s3://{bucket}/{key}")
    count = data['count'].compute()[0]

    # Example processing:
    # We are just going to create some busy work by
    # making a timeseries in the span of 'count' and computing
    # the mean, then returning; something that would be very
    # uncomfortable to do on Lambda.

    # In practice, reading the file, massaging the data then writing
    # to S3, Redshift, etc would be more useful.

    now = datetime.utcnow()
    start = now - timedelta(days=count)
    timeseries = dask.datasets.timeseries(start, now)
    result = timeseries.groupby('x').mean().y.std().compute()
    return result

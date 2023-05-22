import pathlib
import hashlib
from aws_cdk import (
    aws_events as events,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_events_targets as targets,
    App,
    BundlingOptions,
    Duration,
    Stack,
    RemovalPolicy,
)


class DaskLambdaExampleStack(Stack):
    # Bucket to store example data for processing
    bucket: s3.Bucket

    # Dummy producer of files for processing
    lambda_producer: lambda_.Function

    # Example connecting to existing cluster from AWS Lambda
    # and coordinating some work
    lambda_consumer: lambda_.Function

    # Function that starts/stops Dask clusters
    lambda_start_stop_cluster: lambda_.Function

    # Shared file between Dask workers and Lambda client
    dask_processing_layer: lambda_.LayerVersion

    # Dask dependencies for client, (dask, distributed, coiled)
    dask_dependencies_layer: lambda_.LayerVersion

    def __init__(self, app: App, id: str) -> None:
        super().__init__(app, id)

        self.make_bucket()

        self.make_dask_dependencies_layer()
        self.make_dask_processing_layer()

        self.make_lambda_producer()
        self.make_lambda_consumer()
        self.make_lambda_start_stop_cluster()

    def make_bucket(self):
        self.bucket = s3.Bucket(
            self,
            "example-data-bucket",
            access_control=s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

    def make_lambda_producer(self):
        src_file = pathlib.Path(__file__).parent.joinpath("src/lambda_producer.py")

        self.lambda_producer = lambda_.Function(
            self,
            "ExampleProducerFunction",
            code=lambda_.InlineCode(src_file.read_text()),
            handler="index.producer",
            timeout=Duration.seconds(5),
            runtime=lambda_.Runtime.PYTHON_3_10,
            environment={"S3_BUCKET": self.bucket.bucket_name},
        )

        self.lambda_producer.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=[self.bucket.bucket_arn, f"{self.bucket.bucket_arn}/*"],
            )
        )

        # Invoke at regular intervals to produce new files
        rule = events.Rule(
            self, "S3ProducerRule", schedule=events.Schedule.rate(Duration.minutes(1))
        )
        rule.add_target(targets.LambdaFunction(self.lambda_producer))

    def make_lambda_consumer(self):
        src_file = pathlib.Path(__file__).parent.joinpath("src/lambda_consumer.py")

        self.lambda_consumer = lambda_.Function(
            self,
            "ExampleConsumerFunction",
            code=lambda_.InlineCode(src_file.read_text()),
            handler="index.consumer",
            timeout=Duration.seconds(5),
            runtime=lambda_.Runtime.PYTHON_3_10,
            layers=[self.dask_processing_layer, self.dask_dependencies_layer],
        )

        # Trigger consumer
        notification = s3_notifications.LambdaDestination(self.lambda_consumer)
        self.bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification)

    def make_lambda_start_stop_cluster(self):
        src_file = pathlib.Path(__file__).parent.joinpath("src/lambda_consumer.py")

        self.lambda_start_stop_cluster = lambda_.Function(
            self,
            "StartStopClusterFunction",
            code=lambda_.InlineCode(src_file.read_text()),
            handler="index.start_stop_cluster",
            timeout=Duration.seconds(5),
            runtime=lambda_.Runtime.PYTHON_3_10,
            layers=[self.dask_processing_layer, self.dask_dependencies_layer],
        )
        # TODO: Scheduled events to start/stop cluster

    def make_dask_processing_layer(self):
        """
        Layer which bridges Lambda client with code running on workers
        """
        self.dask_processing_layer = lambda_.LayerVersion(
            self,
            "dask_processing_layer",
            code=lambda_.Code.from_asset("layer"),
            description="Code for client and dask workers",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_10],
            removal_policy=RemovalPolicy.DESTROY,
        )

    def make_dask_dependencies_layer(self):
        """
        Layer which bridges Lambda client with code running on workers
        """
        self.dask_dependencies_layer = lambda_.LayerVersion(
            self,
            "dask_dependencies_layer",
            code=lambda_.Code.from_asset(
                "./",
                asset_hash=hashlib.md5(
                    pathlib.Path("./requirements.txt").read_bytes()
                ).hexdigest(),
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_10.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output",
                    ],
                ),
            ),
            description="Dask dependencies (coiled, dask, distributed, etc)",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_10],
            removal_policy=RemovalPolicy.DESTROY,
        )


app = App()
DaskLambdaExampleStack(app, "DaskLambdaExampleStack")
app.synth()

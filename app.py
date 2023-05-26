import pathlib
import hashlib
from aws_cdk import (
    aws_events as events,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_events_targets as targets,
    aws_secretsmanager as secretsmanager,
    CfnParameter,
    SecretValue,
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
        self.coiled_token = CfnParameter(
            self,
            "CoiledToken",
            type="String",
            description="Coiled Token: realistically, already saved and retreive from SecretsManger",
        )
        self.coiled_account = CfnParameter(
            self,
            "CoiledAccount",
            type="String",
            description="Coiled Account: realistically, already saved and retreive from SecretsManger",
        )
        self.coiled_user = CfnParameter(
            self,
            "CoiledUser",
            type="String",
            description="Coiled User: realistically, already saved and retreive from SecretsManger",
        )

        self.make_bucket()
        self.make_secret()

        self.make_dask_dependencies_layer()
        self.make_dask_processing_layer()

        self.make_lambda_producer()
        self.make_lambda_consumer()
        self.make_lambda_start_stop_cluster()

    def make_bucket(self):
        self.bucket = s3.Bucket(
            self,
            "example2-data-bucket",
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # All reading from anything in our account this stack is deployed in
        self.bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:Get*", "s3:List*"],
                resources=[self.bucket.arn_for_objects("*")],
                principals=[
                    iam.AccountPrincipal(self.account),
                ],
            )
        )

    def make_secret(self):
        self.secret = secretsmanager.Secret(
            self,
            "Secret",
            description="Connection information to running Dask Cluster",
            secret_object_value={
                "CLUSTER_NAME": SecretValue.unsafe_plain_text(""),
                "SCHEDULER_ADDR": SecretValue.unsafe_plain_text(""),
                "DASHBOARD_ADDR": SecretValue.unsafe_plain_text(""),
            },
        )

    def make_lambda_producer(self):
        src_file = pathlib.Path(__file__).parent.joinpath("src/lambda_producer.py")

        self.lambda_producer = lambda_.Function(
            self,
            "ExampleProducerFunction",
            description="Example producer function. Generating example data for processing",
            code=lambda_.InlineCode(src_file.read_text()),
            handler="index.producer",
            timeout=Duration.seconds(5),
            runtime=lambda_.Runtime.PYTHON_3_10,
            environment={
                "S3_BUCKET": self.bucket.bucket_name,
                "SECRET_ARN": self.secret.secret_arn,
            },
        )

        # Allow this function to write to s3
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
            description="Example of Dask client from Lambda, coordinating work on remote cluster",
            code=lambda_.InlineCode(src_file.read_text()),
            handler="index.consumer",
            timeout=Duration.seconds(5),
            runtime=lambda_.Runtime.PYTHON_3_10,
            layers=[self.dask_processing_layer, self.dask_dependencies_layer],
            environment={
                "SECRET_ARN": self.secret.secret_arn,
                "DASK_COILED__TOKEN": self.coiled_token.value_as_string,
                "DASK_COILED__ACCOUNT": self.coiled_account.value_as_string,
                "DASK_COILED__USER": self.coiled_user.value_as_string,
            },
        )

        # Get cluster connection info permission
        self.lambda_consumer.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[self.secret.secret_arn],
            )
        )
        self.lambda_consumer.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject"],
                resources=[self.bucket.arn_for_objects("*")],
            )
        )

        # Trigger consumer
        notification = s3_notifications.LambdaDestination(self.lambda_consumer)
        self.bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification)

    def make_lambda_start_stop_cluster(self):
        src_file = pathlib.Path(__file__).parent.joinpath("src/lambda_consumer.py")

        self.lambda_start_stop_cluster = lambda_.Function(
            self,
            "StartStopClusterFunction",
            description="Example of starting and stopping a Dask cluster using coiled",
            code=lambda_.InlineCode(src_file.read_text()),
            handler="index.start_stop_cluster",
            timeout=Duration.seconds(5),
            runtime=lambda_.Runtime.PYTHON_3_10,
            layers=[self.dask_processing_layer, self.dask_dependencies_layer],
            environment={
                "SECRET_ARN": self.secret.secret_arn,
                "DASK_COILED__TOKEN": self.coiled_token.value_as_string,
                "DASK_COILED__ACCOUNT": self.coiled_account.value_as_string,
                "DASK_COILED__USER": self.coiled_user.value_as_string,
            },
        )

        # Update cluster connection info permission
        self.lambda_start_stop_cluster.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:PutSecretValue"],
                resources=[self.secret.secret_arn],
            )
        )
        # Trigger start/stop cluster
        for action, schedule in (
            ("start", "cron(0 7 ? * 1-6 *)"),  # Weekdays 7am
            ("stop", "cron(0 17 ? * 1-6 *)"),  # Weekdays 5pm
        ):
            events.Rule(
                self,
                f"{action.capitalize()}Cluster",
                description=f"Schedule {action} of Dask cluster",
                schedule=events.Schedule.expression(schedule),
                targets=[
                    targets.LambdaFunction(
                        self.lambda_start_stop_cluster,
                        event=events.RuleTargetInput.from_object({"action": action}),
                    )
                ],
            )

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
        command = "pip install --no-cache -r requirements.txt -t /asset-output/python"
        asset_key = pathlib.Path("./requirements.txt").read_bytes() + command.encode()
        self.dask_dependencies_layer = lambda_.LayerVersion(
            self,
            "dask_dependencies_layer",
            code=lambda_.Code.from_asset(
                "./",
                asset_hash=hashlib.md5(asset_key).hexdigest(),
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_10.bundling_image,
                    command=["bash", "-c", command],
                ),
            ),
            description="Dask dependencies (coiled, dask, distributed, etc)",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_10],
            removal_policy=RemovalPolicy.DESTROY,
        )


app = App()
DaskLambdaExampleStack(app, "DaskLambdaExampleStack")
app.synth()

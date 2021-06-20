from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as es
from aws_cdk import aws_lambda_python as lambda_python
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_ssm as ssm
from aws_cdk import core as cdk
from aws_cdk import custom_resources as cr


class RekognitionBatchDetectStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        notification_on: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        collection_id = cdk.CfnParameter(
            self, "RekognitionCollectionId", default="TestCollection"
        )

        # Create a bucket to hold the result of the FaceMatching function
        # To ensure a clean removal of the demo, the bucket and its content are destroyed when deleting the stack
        bk_output = s3.Bucket(
            self,
            "BucketOut",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        self.bucket_out = bk_output

        # Define the Bucket used to host the input data
        # To ensure a clean removal of the demo, the bucket and its content are destroyed when deleting the stack
        bk_test_images = s3.Bucket(
            self,
            "BucketImages",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Define the SQS queue to hold creation events in the images bucket
        queue = sqs.Queue(self, "Queue", visibility_timeout=cdk.Duration.seconds(120))

        # Send notification for object creation events from S3 to the queue
        bk_test_images.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="images/"),
        )

        # Create the Rekognition collection, and ignore if the collection already exists,
        # caling Rekognition API via a Custom Resource

        _ = cr.AwsCustomResource(
            self,
            "RekognitionCollection",
            on_create={
                "service": "Rekognition",
                "action": "createCollection",
                "parameters": {"CollectionId": collection_id.value_as_string},
                "physical_resource_id": cr.PhysicalResourceId.of(
                    "RekognitionCollection"
                ),
                "ignore_error_codes_matching": "ResourceAlreadyExistsException",
            },
            on_delete={
                "service": "Rekognition",
                "action": "deleteCollection",
                "parameters": {"CollectionId": collection_id.value_as_string},
            },
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
        )

        # Define the main lambda function, starting from the library layers
        # (possible alternative is to define a containerized python function, but it's slower to test locally)
        tenacity_lambda_layer = lambda_python.PythonLayerVersion(
            self,
            "TenacityLambdaLayer",
            entry="lambdas/layers/tenacity",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            description="Tenacity",
            layer_version_name="tenacity",
        )
        powertools_lambda_layer = lambda_python.PythonLayerVersion(
            self,
            "AwsLambdaPowerToolsLayer",
            entry="lambdas/layers/aws-lambda-powertools",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            description="AWS-Lambda-Powertools",
            layer_version_name="AWS-Lambda-Powertools",
        )

        # The lambda function proper
        process_fn = lambda_.Function(
            self,
            "MatchFacesFn",
            code=lambda_.Code.from_asset("lambdas/fns/match_faces"),
            handler="lambda.handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={
                "COLLECTION_NAME": collection_id.value_as_string,
                "BUCKET_OUT": bk_output.bucket_name,
                "PREFIX_OUT": "output",
            },
            layers=[tenacity_lambda_layer, powertools_lambda_layer],
            timeout=cdk.Duration.seconds(60),
            reserved_concurrent_executions=5,
        )

        # Setup the trigger for the lambda function. CDK takes care of
        # the relevant IAM policies in the lambda role
        process_fn.add_event_source(es.SqsEventSource(queue))

        # Add read-write privileges to the lambda function to access the buckets
        bk_test_images.grant_read(process_fn)
        bk_output.grant_read_write(process_fn)

        # Grant necessary Rekognition privileges to lambda fn
        process_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:rekognition:{self.region}:{self.account}:collection/{collection_id.value_as_string}"
                ],
                actions=[
                    "rekognition:CompareFaces",
                    "rekognition:DetectFaces",
                    "rekognition:DetectLabels",
                    "rekognition:IndexFaces",
                    "rekognition:ListCollections",
                    "rekognition:ListFaces",
                    "rekognition:SearchFaces",
                    "rekognition:SearchFacesByImage",
                    "rekognition:ListTagsForResource",
                ],
            )
        )

        # Export relevant parameters to SSM to simplify access for out-of-stack
        # applications/scripts
        _ = ssm.StringParameter(
            self,
            "SsmBucketImagesName",
            string_value=bk_test_images.bucket_name,
            parameter_name=f"/{self.stack_name}/ImageBucket",
        )
        _ = ssm.StringParameter(
            self,
            "SsmCollectionId",
            string_value=collection_id.value_as_string,
            parameter_name=f"/{self.stack_name}/CollectionId",
        )
        _ = ssm.StringParameter(
            self,
            "SsmBucketName",
            string_value=bk_output.bucket_name,
            parameter_name=f"/{self.stack_name}/OutBucket",
        )

        if notification_on:
            sns_topic = sns.Topic(
                self, "NotificationTopic", display_name="FaceMatchTopic"
            )

            alarm_threshold = ssm.StringParameter(
                self,
                "NotificationThreshold",
                string_value="90",
                parameter_name=f"/{self.stack_name}/NotificationThreshold",
            )

            # The notification lambda function
            process_fn = lambda_.Function(
                self,
                "NotifyMatchesFn",
                code=lambda_.Code.from_asset("lambdas/fns/notify_matches"),
                handler="lambda.handler",
                runtime=lambda_.Runtime.PYTHON_3_8,
                environment={
                    "BUCKET_OUT": bk_output.bucket_name,
                    "PREFIX_OUT": "output",
                    "NOTIFICATION_THRESHOLD": f"/{self.stack_name}/NotificationThreshold",
                    "TOPIC_ARN": sns_topic.topic_arn,
                },
                layers=[powertools_lambda_layer],
                timeout=cdk.Duration.seconds(60),
                reserved_concurrent_executions=5,
            )
            # cdk.CfnOutput(self, "TestParameterName", value=alarm_threshold.parameter_name)

            sns_topic.grant_publish(process_fn)
            alarm_threshold.grant_read(process_fn)
            bk_output.grant_read(process_fn)

            process_fn.add_event_source(
                es.S3EventSource(
                    bk_output,
                    events=[s3.EventType.OBJECT_CREATED],
                    filters=[s3.NotificationKeyFilter(prefix="output/")],
                )
            )

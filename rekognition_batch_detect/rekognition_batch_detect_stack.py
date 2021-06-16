from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as es
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sqs as sqs
from aws_cdk import core as cdk
from aws_cdk import custom_resources as cr
from aws_cdk.aws_lambda_python import PythonLayerVersion


class RekognitionBatchDetectStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        collection_id = cdk.CfnParameter(
            self, "RekognitionCollectionId", default="TestCollection"
        )

        # Define the main SQS queue
        queue = sqs.Queue(self, "Queue")

        # Define the Bucket used to host the data
        bk_test_images = s3.Bucket(self, "Bucket with Test Images")

        # Send notification for object creation events from S3 to the queue
        bk_test_images.add_event_notification(
            event=s3.EventType.OBJECT_CREATED, dest=s3n.SqsDestination(queue)
        )

        # Create the Rekognition collection, and ignore if the collection already exists
        aws_rekognition_cr = cr.AwsCustomResource(
            self,
            "CR:RekognitionCollection",
            on_create={
                "service": "Rekognition",
                "action": "createCollection",
                "parameters": {"CollectionId": collection_id.to_string()},
                "physical_resource_id": cr.PhysicalResourceId.of(
                    "RekognitionCollection"
                ),
                "ignore_error_codes_matching": "ResourceAlreadyExistsException",
            },
            on_delete={
                "service": "Rekognition",
                "action": "deleteCollection",
                "parameters": {"CollectionId": collection_id.to_string()},
            },
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
        )

        # Define the main lambda function, starting from the libraries layer
        # (possible alternative is to define a containerized python function, but it's slower to test locally)
        tenacity_lambda_layer = PythonLayerVersion(
            self,
            "TenacityLambdaLayer",
            entry="fns/libs",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            description="Tenacity and AWS-Lambda-Powertools",
            layer_version_name="1",
        )

        # The lambda function proper
        process_fn = lambda_.Function(
            self,
            "ConsumerFn",
            code=lambda_.Code.from_asset("fns"),
            handler="lambda.handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={"COLLECTION_NAME": collection_id.to_string()},
            layers=[
                tenacity_lambda_layer,
            ],
            timeout=cdk.Duration.seconds(60),
            reserved_concurrent_executions=5,
        )

        # Setup the trigger for the lambda function. CDK takes care of
        # the relevant IAM policies in the lambda role
        process_fn.add_event_source(es.SqsEventSource(queue))

        # Add read-write privileges to the lambda function to access the bucket
        bk_test_images.grant_read_write(process_fn)

        # Grant necessary Rekognition privileges to lambda fn
        process_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:rekognition:{self.region}:{self.account}:collection/{collection_id.to_string()}"
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

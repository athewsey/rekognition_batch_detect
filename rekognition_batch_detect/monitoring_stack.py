from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import core as cdk
from aws_cdk import custom_resources as cr


class MonitoringStack(cdk.Stack):
    def __init__(
        self, scope: cdk.Construct, construct_id: str, data_bucket: s3.Bucket, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        face_match_db = glue.Database(
            self, "FaceMatchResultsDatabase", database_name="face_match_output_db"
        )

        glue_role = iam.Role(
            self,
            "Glue Role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                resources=[data_bucket.bucket_arn],
                actions=[
                    "s3:Get*",
                    "s3:List*",
                ],
            )
        )

        face_match_table = glue.Table(
            self,
            "FaceMatchResultsTable",
            database=face_match_db,
            table_name="face_match_output",
            columns=[
                {"name": "source", "type": glue.Schema.STRING},
                {"name": "customerid", "type": glue.Schema.STRING},
                {
                    "name": "matches",
                    "type": glue.Schema.array(
                        input_string="struct<Similarity:double,Face:struct<FaceId:string,BoundingBox:struct<Width:double,Height:double,Left:double,Top:double>,ImageId:string,ExternalImageId:string,Confidence:double>,CustomerId:string>",
                        is_primitive=False,
                    ),
                },
            ],
            data_format=glue.DataFormat.JSON,
            bucket=data_bucket,
            s3_prefix="output/",
        )

        # Create a view in athena to unnest the matches array
        athena_view = cr.AwsCustomResource(
            self,
            "AthenaView",
            on_create={
                "service": "Athena",
                "action": "startQueryExecution",
                "parameters": {
                    "QueryString": view_query.format(
                        **{
                            "database": face_match_db.database_name,
                            "table": face_match_table.table_name,
                        }
                    ),
                    "QueryExecutionContext": {"Database": face_match_db.database_name},
                    "ResultConfiguration": {
                        "OutputLocation": f"s3://{data_bucket.bucket_name}/queries"
                    },
                },
                "physical_resource_id": cr.PhysicalResourceId.of("AthenaView"),
            },
            install_latest_aws_sdk=False,
            policy=cr.AwsCustomResourcePolicy.from_statements(
                [
                    iam.PolicyStatement(
                        actions=[
                            "glue:GetTable",
                            "glue:CreateTable",
                            "athena:StartQueryExecution",
                        ],
                        resources=[
                            face_match_db.catalog_arn,
                            f"arn:aws:athena:ap-southeast-1:{self.account}:workgroup/primary",
                        ],
                    ),
                    iam.PolicyStatement(
                        actions=["s3:*"],
                        resources=[data_bucket.bucket_arn],
                    ),
                ]
            ),
        )


view_query = """CREATE OR REPLACE VIEW matchingstats AS 
SELECT
  customerid
, match.similarity
, match.customerid suspect_match
FROM
  ({database}.{table}
CROSS JOIN UNNEST(matches) t (match))
"""

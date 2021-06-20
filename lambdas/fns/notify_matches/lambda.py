import json
import os
import urllib.parse
from collections import Counter

import boto3
from aws_lambda_powertools import Logger

logger = Logger()
s3 = boto3.resource("s3")
ssm = boto3.client("ssm")
sns = boto3.client("sns")

bucket_out = os.getenv("BUCKET_OUT")
prefix_out = os.getenv("PREFIX_OUT", "output")
topic_arn = os.getenv("TOPIC_ARN")
threshold_param_name = os.getenv("NOTIFICATION_THRESHOLD")
print(threshold_param_name)

# @logger.inject_lambda_context(log_event=True)
def handler(event, context):
    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )

        threshold = ssm.get_parameters(Names=[threshold_param_name])["Parameters"][0][
            "Value"
        ]
        threshold = float(threshold)

        file_object = s3.Object(bucket, key)
        report = json.loads(file_object.get()["Body"].read())

        response_list = parse_and_publish(topic_arn, report, threshold)
        logger.info(f"Process finished, sent {len(response_list)} alert messages")

        return {"status": "success", "message": json.dumps(response_list)}
    except Exception:
        logger.exception("Received an exception")


def parse_and_publish(topic_arn, report: dict, threshold: float):

    # parse the report to extract matches above threshold
    name_matches = [
        m["Face"]["ExternalImageId"].rsplit("_", 1)[0]
        for m in report["Matches"]
        if m["Similarity"] >= threshold
    ]
    # Use a counter to only notify of the number of matches per ID
    name_counter = Counter(name_matches)

    match_dict = {
        k: max(
            [
                m["Similarity"]
                for m in report["Matches"]
                if k in m["Face"]["ExternalImageId"]
            ]
        )
        for k in name_counter
    }

    messages = [
        f"{report['CustomerID']}, image {report['Source']}, "
        f"matched {name} {name_counter.get(name)} times with max similarity {sim:.3f}"
        for name, sim in match_dict.items()
    ]
    responses = [sns.publish(TopicArn=topic_arn, Message=m) for m in messages]
    return [r["MessageId"] for r in responses]

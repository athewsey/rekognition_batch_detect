import json
import os

import boto3
from aws_lambda_powertools import Logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = Logger()
reko = boto3.client("rekognition")
s3 = boto3.resource("s3")
collection_id = os.getenv("COLLECTION_NAME")
max_faces_index = os.getenv("MAX_FACE_INDEX", 1)
max_faces_match = os.getenv("MAX_FACE_MATCH", 100)
threshold = os.getenv("FACE_MATCHING_THRESHOLD", 50)
bucket_out = os.getenv("BUCKET_OUT")
prefix_out = os.getenv("PREFIX_OUT", "output")

# @logger.inject_lambda_context(log_event=True)
def handler(event, context):
    try:
        for record in event["Records"]:
            payload = record["body"]
            for r in json.loads(payload)["Records"]:
                # Extract the reference of the image that triggered the lambda
                bucket = r["s3"]["bucket"]["name"]
                object_key = r["s3"]["object"]["key"]
                logger.info(f"bucket:{bucket}, key:{object_key}")

                # Extract semantinc info form the object key
                image_id = object_key.split("/")[-1]
                customer_id = image_id.rsplit("_", 1)[0]

                # add image to collection and search for matches
                matches = add_search_twin(
                    bucket,
                    object_key,
                    image_id=image_id,
                    collection_id=collection_id,
                )
                logger.info(f"Found {len(matches)} matches in the collection")
                logger.info(matches)

                # check if there's any match with other customers_id
                res = [
                    {**k, "CustomerId": k["Face"]["ExternalImageId"].rsplit("_", 1)[0]}
                    for k in matches
                    if k["Face"]["ExternalImageId"].rsplit("_", 1)[0] != customer_id
                ]

                logger.info(f"Matches that require manual investigation {res}")

                # if len(res) > 0:
                s3object = s3.Object(bucket_out, f"{prefix_out}/{image_id}.json")
                s3object.put(
                    Body=(
                        bytes(
                            json.dumps(
                                {
                                    "Source": f"{bucket}/{object_key}",
                                    "CustomerID": customer_id,
                                    "Matches": res,
                                }
                            ).encode("UTF-8")
                        )
                    )
                )

        return {"status": "success", "message": res}
    except Exception:
        logger.exception("Received an exception")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.2, min=1, max=10))
def search_faces_retry(**kwargs):
    return reko.search_faces(**kwargs)


def add_search_twin(bucket, s3_path, image_id, collection_id):
    response_add = reko.index_faces(
        CollectionId=collection_id,
        Image={"S3Object": {"Bucket": bucket, "Name": s3_path}},
        ExternalImageId=image_id,
        MaxFaces=max_faces_index,
        QualityFilter="AUTO",
        DetectionAttributes=["DEFAULT"],
    )
    try:
        face_id = response_add["FaceRecords"][0]["Face"]["FaceId"]
    except:
        print("no face detected in image")
        return

    response_search = search_faces_retry(
        CollectionId=collection_id,
        FaceId=face_id,
        FaceMatchThreshold=threshold,
        MaxFaces=max_faces_match,
    )

    faceMatches = response_search["FaceMatches"]
    return faceMatches

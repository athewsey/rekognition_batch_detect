import json

import s3fs
from IPython.display import Image, display
import boto3

s3 = s3fs.S3FileSystem()
client = boto3.client("rekognition")

def display_s3(uri):
    with s3.open(uri) as f:
        display(Image(f.read()))


def inspect_matches(bucket_name, report_json):
    with s3.open(report_json) as f:
        report = json.load(f)
    source_uri = report["Source"]
    customer_id = report['CustomerID']
    matches = [
        {
            "uri": f"{bucket_name}/images/{k['CustomerId']}/{k['Face']['ExternalImageId']}",
            "Similarity": k["Similarity"],
            "CustomerID": k["Face"]["ExternalImageId"].rsplit("_", 1)[0],
        }
        for k in report["Matches"]
    ]
    print(f"Source Image: {source_uri}, CustomerID: {customer_id}")
    display_s3(source_uri)
    print("Identified Matches")
    for m in matches:
        print("CustomerID: {CustomerID},\nSimilarity: {Similarity:.3f}\nImage: {uri}".format(**m))
        display_s3(m["uri"])

        
def count_hits(uri):
    with s3.open(uri) as f:
        example = json.load(f)
        return len(example['Matches'])
        

def reset_collection(collection_id):
    response_0 = client.delete_collection(CollectionId=collection_id)
    response_1 = client.create_collection(CollectionId=collection_id)
    return response_0, response_1
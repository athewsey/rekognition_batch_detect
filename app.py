#!/usr/bin/env python3
import os

from aws_cdk import core as cdk

from rekognition_batch_detect.rekognition_batch_detect_stack import (
    RekognitionBatchDetectStack,
)


app = cdk.App()
RekognitionBatchDetectStack(
    app,
    "RekognitionBatchDetectStack",
)

app.synth()

#!/usr/bin/env python3
import os

from aws_cdk import core as cdk

from rekognition_batch_detect.base_stack import (
    RekognitionBatchDetectStack,
)
from rekognition_batch_detect.monitoring_stack import MonitoringStack


app = cdk.App()
base_stack = RekognitionBatchDetectStack(
    app,
    "RekognitionBatchDetect",
    notification_on=True
)

MonitoringStack(app, "MonitoringStack", base_stack.bucket_out)
app.synth()

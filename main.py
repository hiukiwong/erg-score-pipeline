import boto3
from cli_entry import *
from testprocess import *


## AWS set up
boto3.setup_default_session(profile_name='dev-profile')
bucket = 'ergscorepipeline'


## Obtain workout data from user input
date, segments, segment_length = get_workout_paramters()
erg_scores = get_all_splits(segments, segment_length)
## Generate key for uploading to s3 bucket
key = f"raw-data/manual-input/{date}.csv"
## Upload raw workout data to s3 bucket
upload_to_s3(date, erg_scores, bucket, key)
import csv
from pathlib import Path
import os
import pandas as pd
import boto3
from io import StringIO

#AWS set up
boto3.setup_default_session(profile_name='dev-profile')
client = boto3.client("s3")
bucket = 'ergscorepipeline'



#TODO: tidy up and refactor code

#Get parameters
date = input('Enter the date of the workout in YYYY-MM-DD: ')
x = [int(x) for x in input("Enter the duration of the workout in minutes, and the number of segments the workout has been dvided into: ").split(",")]

duration = x[0]
segments = x[1]
segment_lenth = int(x[0]/x[1])

erg_scores = []
segment_counter = 1
while segment_counter <= segments:
    x, y, z = input(f"Enter the metres rowed, split, and the stroke rate for segment {segment_counter}: ").split(',')
    erg_scores.append((segment_lenth*segment_counter,x,y,z))
    segment_counter +=1

#Convert inputs directly to dataframe, saving directly to s3 bucket
erg_scores_df = pd.DataFrame(erg_scores, columns =['cutoff_minute', 'metres', '/500m', 's/m'])
csv_buffer = StringIO()
erg_scores_df.to_csv(csv_buffer, index = False)

response = client.put_object(Bucket=bucket, Key=f"raw-data/manual-input/{date}.csv", Body=csv_buffer.getvalue())
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successfully uploaded erg score from {date}. Status - {status}")
else:
    print(f"Upload to S3 unsuccessful. Status - {status}")
from pathlib import Path
import pandas as pd
from io import StringIO
import boto3

import pandera as pa
from pandera import Column, DataFrameSchema, Check, check_output

cli_input_schema = DataFrameSchema({
    'cutoff_minute': Column(int, coerce=True),
    'metres': Column(
        int, Check(lambda x: 1000 <= x <= 2000, element_wise=True,
                   error="range checker [1000, 2000]"), coerce=True),
    's/m': Column(
        int, Check(lambda x: 18 <= x <= 23, element_wise=True,
                   error="range checker [18, 23]"), coerce=True),
    '/500m': Column(str, Check.str_matches("\d\:[0-5]\d\.\d"))
})

#TODO: Refactor code

#Get parameters
def get_workout_paramters():
    date = input('Enter the date of the workout in YYYY-MM-DD: ')
    x = [int(x) for x in input("Enter the duration of the workout in minutes, and the number of segments the workout has been dvided into: ").split(",")]
    duration = x[0]
    segments = x[1]
    segment_length = int(x[0]/x[1])
    return date, segments, segment_length

def get_all_splits(segments, segment_length):
    erg_scores = []
    segment_counter = 1
    while segment_counter <= segments:
        x, y, z = input(f"Enter the metres rowed, split, and the stroke rate for segment {segment_counter}: ").split(',')
        erg_scores.append((segment_length*segment_counter,x,y,z))
        segment_counter +=1
    return erg_scores

@check_output(cli_input_schema)
def upload_to_s3(date, erg_scores, bucket, key):
#Convert inputs directly to dataframe, saving directly to s3 bucket
    client = boto3.client("s3")
    erg_scores_df = pd.DataFrame(erg_scores, columns =['cutoff_minute', 'metres', '/500m', 's/m'])
    csv_buffer = StringIO()
    erg_scores_df.to_csv(csv_buffer, index = False)

    response = client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successfully uploaded erg score from {date}. Status - {status}")
    else:
        print(f"Upload to S3 unsuccessful. Status - {status}")
    return erg_scores_df
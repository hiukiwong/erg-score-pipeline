import pandas as pd
from io import StringIO, BytesIO
import pandera as pa
from pandera import Column, DataFrameSchema, check_output
import boto3
# boto3.setup_default_session(profile_name='dev-profile')
s3_client = boto3.client("s3")

input_bucket = 'ergscorepipeline'
output_bucket = 'ergscorepipeline-processed'
#TODO: Refactor code, add docstrings

processed_schema = DataFrameSchema({
    'workout_date': Column(pa.engines.pandas_engine.DateTime(tz='Europe/London')),
    'cutoff_minute': Column(int, coerce=True),
    'split_in_s': Column(float, coerce=True),
})

def split_to_seconds(duration_string: str):
    return float(duration_string.split(':')[0])*60+float(duration_string.split(':')[1])

@check_output(processed_schema)
def process_locally(bucket, date):
    input_key = f'raw-data/manual-input/{date}.csv'
    obj = s3_client.get_object(Bucket=bucket, Key=input_key)
    erg_scores_df = pd.read_csv(BytesIO(obj['Body'].read()))
    erg_scores_df["split_in_s"] = erg_scores_df["/500m"].apply(lambda x: split_to_seconds(x))
    erg_scores_df['workout_date'] = pd.to_datetime(date).tz_localize('Europe/London')
    return erg_scores_df, input_key


@check_output(processed_schema)
def process_lambda(erg_scores_df, date):
    erg_scores_df["split_in_s"] = erg_scores_df["/500m"].apply(lambda x: split_to_seconds(x))
    erg_scores_df['workout_date'] = pd.to_datetime(date).tz_localize('Europe/London')
    
    csv_buffer = StringIO()
    erg_scores_df.to_csv(csv_buffer, index = False)
    output_key = f'{date}.csv'
    response = s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
            print(f"Successfully processed erg score from {date}. Status - {status}")
    else:
            print(f"Upload to S3 unsuccessful. Status - {status}")
    return erg_scores_df

def lambda_handler(event, context): 
    #Get bucket and file name
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    #Get object
    response = s3_client.get_object(Bucket=bucket, Key=key)

    #Process it
    erg_scores_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    date = key[-14:-4]
    process_lambda(erg_scores_df, date)
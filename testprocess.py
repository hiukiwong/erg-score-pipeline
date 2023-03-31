import pandas as pd
import os
import io
import pandera as pa
from pandera import Column, DataFrameSchema, Check, check_output
import boto3
boto3.setup_default_session(profile_name='dev-profile')
client = boto3.client("s3")

bucket = 'ergscorepipeline'
key = f'raw-data/manual-input/{date}.csv'

processed_schema = pa.DataFrameSchema({
    'workout_date': pa.Column(pa.engines.pandas_engine.DateTime(tz='Europe/London')),
    'cutoff_minute': pa.Column(int, coerce=True),
    'split_in_s': pa.Column(float, coerce=True),
})

def split_to_seconds(duration_string: str):
    return float(duration_string.split(':')[0])*60+float(duration_string.split(':')[1])

@check_output(processed_schema)
def process_locally(bucket, key, date):
    obj = client.get_object(Bucket=bucket, Key=key)
    erg_scores_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    erg_scores_df["split_in_s"] = erg_scores_df["/500m"].apply(lambda x: split_to_seconds(x))
    erg_scores_df['workout_date'] = pd.to_datetime(date).tz_localize('Europe/London')
    return erg_scores_df



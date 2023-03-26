import pandas as pd
import os
import io
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import boto3
boto3.setup_default_session(profile_name='dev-profile')
client = boto3.client("s3")

bucket = 'ergscorepipeline'
key = f'raw-data/manual-input/{date}.csv'


def split_to_seconds(duration_string: str):
    return float(duration_string.split(':')[0])*60+float(duration_string.split(':')[1])

def process():
    obj = client.get_object(Bucket=bucket, Key=key)
    erg_score = pd.read_csv(io.BytesIO(obj['Body'].read()))
    erg_score["split_in_s"] = erg_score["/500m"].apply(lambda x: split_to_seconds(x))
    erg_score['cutoff_minute'] = erg_score['time'].str.split(':').str[0].astype(int)
    erg_score['workout_type'] = self.workout_type
    erg_score['workout_date'] = pd.to_datetime(self.input_filename[-14:-4]).dt.tz_localize('Europe/London')


allowed_workout_types = ['A', 'B']

schema = pa.DataFrameSchema({
    'workout_date': pa.Column(pa.engines.pandas_engine.DateTime(tz='Europe/London')),
    'workout_type': pa.Column(pa.Category, checks = pa.Check.isin(allowed_workout_types), coerce=True, regex = True),
    'cutoff_minute': pa.Column(int, coerce=True),
    'split_in_s': pa.Column(float, coerce=True),
    'meter': pa.Column(int, coerce=True)
})


class ErgScoreValidator:
    pass
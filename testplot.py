import numpy as np
from datetime import time, datetime, timedelta
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator
import matplotlib.ticker as ticker
from matplotlib.dates import DateFormatter
import matplotlib
import pandas as pd

class ErgScorePlotter:
    def __init__(self, data_path):
        self.data_path = data_path
        self.df = pd.read_csv(data_path)

    def generate_pb_stats(self):
        averages = self.df.groupby(['workout_date']).split_in_s.mean()
        self.df = pd.merge(self.df, averages, how='outer', on='workout_date')
        self.df = self.df.rename(columns={'split_in_s_y': 'average_split', 'split_in_s_x':'split_in_s'})
        print('You recorded your fastest average split for this workout on '+self.df.loc[self.df['average_split'].idxmin()]['workout_date'][:10]+'. The average split was '+str(timedelta(seconds=self.df.average_split.min()))[3:10]+'.')
        return averages

    def compare_against_pb(self):
        pass
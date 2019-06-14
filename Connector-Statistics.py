#Import libraries
import os
from datetime import datetime,date
from pandas import DataFrame,Series
import datetime
import sys, re
import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
import scipy as sp
import numpy as np, scipy.stats as st
import math

conflength = pd.read_csv('Length-288.csv', converters={'Length': lambda x: str(x)})

#Specify IMPALA_HOST as an environment variable in your project settings
IMPALA_HOST = os.getenv('IMPALA_HOST', '<lake_server_name>')
#Connect to Impala using Impyla
conn = connect(host=IMPALA_HOST, port=<Port>, auth_mechanism='<auth type>', use_ssl=True)
cursor = conn.cursor()
#Get the available tables
cursor.execute('SHOW TABLES')
tables = as_pandas(cursor)

#Query Data for Connector statistics
cursor.execute('select substring(end_time,1,10) as dateString,substring(end_time,15,5) as hourString, connector, event_count from connector_table \
where end_time BETWEEN concat(to_date(now() - interval 12 days), " 00:00:00") and concat(to_date(now() - interval 5 days), " 24:00:00") and name = "Connector Statistics FWD" \
group by substring(end_time,1,10), substring(end_time,15,5), connector, event_count \
order by 1,2')
last_7_days_connector = as_pandas(cursor)               

#Round down to nearest hour/min
last_7_days_connector['hourstring'] = last_7_days_connector['hourstring'].str[:-1]
#Add minute and second > 0:00
last_7_days_connector['hourstring'] = last_7_days_connector['hourstring'] + '0:00'

# WORK WITH YESTERDAY CONNECTOR DATA
#Define yesterday
yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-6).strftime("%F")
#Make a boolean mask
mask = (last_7_days_connector['datestring'] == yesterday)
#Re-assign data to a dataframe
yesterday_connector = last_7_days_connector.loc[mask]
#Remove datestring
yesterday_connector.drop(['datestring'], axis=1, inplace=True)
#Convert hourstring to string
yesterday_connector['hourstring'] = yesterday_connector['hourstring'].astype(str)
#Convert flexstring to string
yesterday_connector['connector'] = yesterday_connector['connector'].astype(str)
#Convert eventcount to float
yesterday_connector['event_count'] = yesterday_connector['event_count'].astype(float)
#Yesterday mean per 10 min
yesterday_connector_mean = yesterday_connector.groupby(['hourstring','connector'], as_index=False).agg({'event_count': 'mean'})
#Rename columns
yesterday_connector_mean.columns = ['hour', 'connector', 'yesterday']
#Convert to int
yesterday_connector_mean['yesterday'] = yesterday_connector_mean['yesterday'].astype(int)

# WORK WITH WEEK CONNECTOR DATA
#Remove datestring
last_7_days_connector.drop(['datestring'], axis=1, inplace=True)
#Convert hourstring to string
last_7_days_connector['hourstring'] = last_7_days_connector['hourstring'].astype(str)
#Convert flexstring to string
last_7_days_connector['connector'] = last_7_days_connector['connector'].astype(str)
#Convert eventcount to float
last_7_days_connector['event_count'] = last_7_days_connector['event_count'].astype(float)

#MEAN
#Group by hour and connector and get mean per 10 min
connector_mean = last_7_days_connector.groupby(['hourstring','connector'], as_index=False).agg({'event_count': 'mean'})
#Convert from float to int
connector_mean['event_count'] = connector_mean['event_count'].astype(int)
#Rename columns
connector_mean.columns = ['hour', 'connector', 'mean']

#STD
#Group by hour and connector and get std per 10 min
connector_std = last_7_days_connector.groupby(['hourstring','connector'], as_index=False).agg({'event_count': 'std'})
#Replace NaN values with 0's
connector_std['event_count'].fillna(0, inplace=True)
#Convert from float to int
connector_std['event_count'] = connector_std['event_count'].astype(int)
#Rename columns
connector_std.columns = ['hour', 'connector', 'std']

#MERGE DATAFRAMES
merged_connector = connector_mean.merge(connector_std, how = 'inner', on = ['hour', 'connector'])

#Convert to float
connector_mean['mean'] = connector_mean['mean'].astype(float)
connector_std['std'] = connector_std['std'].astype(float)

#Get 95 % confidence
confidence = stats.norm.interval(0.95,loc=connector_mean['mean'],scale=connector_std['std']/math.sqrt(len(conflength)))

#Convert to dataframe
confidence_up_low = pd.DataFrame(np.column_stack(confidence),columns=['95_lower_conf','95_upper_conf'])

#Replace NaN values with 0's
confidence_up_low['95_lower_conf'].fillna(0, inplace=True)
confidence_up_low['95_upper_conf'].fillna(0, inplace=True)

#Convert back to int
confidence_up_low['95_lower_conf'] = confidence_up_low['95_lower_conf'].astype(int)
confidence_up_low['95_upper_conf'] = confidence_up_low['95_upper_conf'].astype(int)


#Merge dataframes for 7 days
connector_merge_all = pd.concat([merged_connector, confidence_up_low], axis=1)

#Merge in yesterday dataframe
connector_merge_all = pd.merge(connector_merge_all, yesterday_connector_mean, on = ['hour', 'connector'])

######### EXPORT TO CSV ###########
connector_merge_all.to_csv('connector_mean_std_95_new.csv', index=False, header=True)

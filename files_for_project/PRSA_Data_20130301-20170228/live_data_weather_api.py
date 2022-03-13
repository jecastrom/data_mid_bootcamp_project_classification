#!/usr/bin/env python
# coding: utf-8

# ## Data Ingestion from the Weather API to InfluxDB on AWS EC2 instance

# This are the steps we go through in this script:
#
# * To query the API with the "request" library as specified in the Weather API documentation https://www.weatherapi.com/docs/
# * Format the timestamp
# * "Normalize" or flatten in tabular form the json object with the json_normalize() function
# * Rename the columns
# * Filter out the columns we need on a DataFrame
# * Set up the connection string and call the API using the InfluxDB client https://docs.influxdata.com/influxdb/cloud/api-guide/client-libraries/python/
#
#
# * Write the data into InfluxDB
#
# https://www.influxdata.com/blog/writing-data-to-influxdb-with-python/
#
# https://docs.influxdata.com/influxdb/cloud/api-guide/client-libraries/python/
#

# In[40]:


from numpy import float64, int32, string_
import requests
import json
import pandas as pd
from pandas import json_normalize
import datetime as dt


# In[41]:


# Import the influxdb API client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


# In[42]:


# load the configuration from the json file (Having the API key on a separate file, we are not
# exposing the key on the code publicly)
with open("api_config.json") as json_data_file:
    config = json.load(json_data_file)


# In[43]:


# The Payload of an API Module is the body of your request and response message. It contains the data that
# you send to the server when you make an API request. You can send and receive Payload in
# different formats, for instance JSON.

# Here we can send the variable "Key" which contains the API key being read from the json file
# The "q" is a query parameter of the API based on the location
# "aqi" is a parameter to enable or desable to receive air quality data in forecast API output
# In the API documentation we obtain and compose the url to request the "current json" using the "request" library
payload = {'Key': config['Key'], 'q': 'Berlin', 'aqi': 'yes'}
r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)


# In[44]:


# Here we create an object called r_string with the request and apply the .json() function to create
# the json file. If the responce is not written in json format, it would give us an error message.
r_string = r.json()


# In[45]:


# In the previous cell, we can see the responce, we obtain a json file with nested fields. So for us to
# deal effectibly with this data, it is best to deal with a table layout, where we can have column names.
# So we normalize the nested json with the json_normalize() function. To "normalize" in this context is to convert
# the nested json or this  semi-structured JSON data into tabular data, a flat table
normalized = json_normalize(r_string)


# In[46]:


# Adding a new column 'TimeStamp'
# Transforming the time from "localtime_epoch" to YYYY-MM-DDTHH:MM:SS format
# From Weather API we only get time in "local" time format, not in UTC time format. This is the
# reason why we have to have this timestamp format
# with +02.00 which is very important (for Berlin)
# otherwise TS will be in UTC and therefore in the future -> it will not get shown on the board
normalized['TimeStamp'] = normalized['location.localtime_epoch'].apply(
    lambda s: dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+02:00'))


# In[47]:


# after the json "normalization" we have to rename the columns with names meaninful to us.
normalized.rename(columns={'location.name': 'location', 'location.region': 'region', 'current.temp_c': 'temp_c',
                           'current.wind_kph': 'wind_kph', 'current.pressure_mb': 'PRES', 'current.precip_mm': 'RAIN', 'current.humidity': 'HUMID',
                           'current.air_quality.pm2_5': 'PM2_5', 'current.air_quality.pm10': 'PM10', 'current.air_quality.so2': 'SO2',
                           'current.air_quality.no2': 'NO2', 'current.air_quality.co': 'CO', 'current.air_quality.o3': 'O3'

                           }, inplace=True)
print(normalized)


# In[48]:


# Here again, we must set the timestamp as the index as its the norm for our time series database
normalized.set_index('TimeStamp', inplace=True)


# In[49]:


# Defining tag fields
datatags = ['location']


# In[50]:


# The ex_df is the final DataFrame to export or to write into the database
# Also we filter out just the columns that we are going to export, in our case, temp and wind for export.
# So we select only the data we need from the whole bunch of data that we fetched from the API response.
ex_df = normalized.filter(['location', 'temp_c', 'wind_kph', 'PRES',
                          'RAIN', 'HUMID', 'PM2_5', 'PM10', 'SO2', 'NO2', 'CO', 'O3'])

print(ex_df)


# In[51]:


# Setting up Database (Connection string) using the InfluxDB client
client = influxdb_client.InfluxDBClient(
    url='ec2-3-120-10-156.eu-central-1.compute.amazonaws.com:8086',
    token='kp_gJV8Y7aOZB0odriHj1o_qWurQmxzBXpTU_kYY4JnQ88e9X1TI3ifFVA1pUyGZshZfTpeaTlTWgYAUESTMvw==',
    org='my-org'
)


# In[52]:


# write the data to the database InfluxDB into measurement
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='live_weather', org='my-org', record=ex_df,
                          data_frame_measurement_name='location-only', data_frame_tag_columns=['location'])
write_api.flush()
print('Errors detected writing data to InfluxDB: ', message)


# In[ ]:

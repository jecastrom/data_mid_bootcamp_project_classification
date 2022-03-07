#!/usr/bin/env python
# coding: utf-8

# ### Automating the execution of the Data Ingestion from the Weather API to InfluxDB on AWS EC2 instance

# In[31]:


import time
import schedule
from numpy import float64, int32, string_
import requests
import json
import pandas as pd
from pandas import json_normalize
import datetime as dt


# In[32]:


def run_script():
    with open('live_data_weather_api.py') as f:
        script = f.read()
    exec(script)
    print('Script executed successfully!!')


schedule.every(60).minutes.do(run_script)

while True:
    schedule.run_pending()
    time.sleep(1)


# In[ ]:

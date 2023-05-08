import requests
import json
import datetime,time
from time import sleep
import mysql.connector
from datetime import datetime
import tzlocal
import logging

import Class_Definition

request_timestamp = 1674191700000/1000
local_timezone = tzlocal.get_localzone() # get pytz timezone
request_timestamp_local_time = datetime.fromtimestamp(request_timestamp, local_timezone).strftime("%Y-%m-%d %H:%M:%S")
print(request_timestamp_local_time)
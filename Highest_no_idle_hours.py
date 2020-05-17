from pyhive import hive
import pandas as pd
import sys
# Establish  connection between hive server and Database
conn = hive.Connection(host="localhost", port=10000, database="logs", auth="NOSASL")
try:

    query = pd.read_sql('select * from cpulogs', conn)  #Load Database into Hive

    query['cpulogs.idle_time'] = pd.to_datetime(query['cpulogs.idle_time'])   #convert the data into the format of datetime
   idledata =query[query['cpulogs.idle_time'] > query['cpulogs.idle_time'].mean()]   #calculate total idle_hours mean
    print(idledata)

    HIGHEST_NO_IDLE_HOURS=idledata['cpulogs.user_name']   #print user_name with HIGHEST_NO_IDLE_HOURS
    print(HIGHEST_NO_IDLE_HOURS)
except:
    print("Syntax error")
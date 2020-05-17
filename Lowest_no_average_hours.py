from pyhive import hive
import pandas as pd
import sys
#Establish  connection between Hive server and Database
conn = hive.Connection(host="localhost", port=10000, database="logs", auth="NOSASL")


query= pd.read_sql('select * from cpulogs', conn)  #Load database into hive
query['cpulogs.working_hours'] = pd.to_datetime(query['cpulogs.working_hours']) #convert the data into the format of datetime

avg_hour = query[query['cpulogs.working_hours'] < query['cpulogs.working_hours'].mean()]  #calculate the mean of total working_hours
print(avg_hour)

LOWEST_NO_AVERAGE_HOURS =avg_hour['cpulogs.user_name']  #print the user_name with lowest no of average hours
print(LOWEST_NO_AVERAGE_HOURS)
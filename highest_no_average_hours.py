from pyhive import hive
import pandas as pd
import sys
#Establish Connection between Hive Server and Database

conn = hive.Connection(host="localhost", port=10000, database="logs", auth="NOSASL")

query= pd.read_sql('select * from cpulogs', conn)    #Load database into Hive

query['cpulogs.working_hours'] = pd.to_datetime(query['cpulogs.working_hours'])  #convert the data into the format of date_time

averagehours = query[query['cpulogs.working_hours'] > query['cpulogs.working_hours'].mean()]  #calculate the mean of total working_hours
print(averagehours)

highest_no_average_hours =averagehours['cpulogs.user_name'] #print the user_name with highest no of average hours
print(highest_no_average_hours)
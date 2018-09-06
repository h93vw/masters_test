from pyspark_cassandra import CassandraSparkContext
from meter_selection import get_meters
from pyspark import SparkContext, SQLContext
from pyspark.sql import functions as F
import numpy as np
import pandas as pd
import time as t
import datetime
import random

# from pyspark import SparkConf
# from cassandra.cluster import Cluster

def MakeList(x):
    T = tuple(x)
    if len(T) > 1:
        return T
    else:
        return T[0]

## PYTHON3 ##
import os
os.environ["PYSPARK_PYTHON"] = 'C:\\ProgramData\\Anaconda3\\python.exe'
os.environ["PYSPARK_DRIVER_PYTHON"] = 'C:\\ProgramData\\Anaconda3\\python.exe'

start_time = datetime.datetime(2010, 10, 7, 0, 0)
sample_frequency = datetime.timedelta(minutes=30)
num_tests = 2
list_times = [start_time + x * sample_frequency for x in range(num_tests)]
num_of_meters = 30
window_size = datetime.timedelta(hours=24)
init_model_params = {}
meter_ids = get_meters()
mk = 3
lrate = 0.75
SE = 0
i = 1

program_start_time = t.time()

sc = CassandraSparkContext(appName="PySpark Cassandra Test", master="local[*]")

'''DataFrame Tests'''
#for current_time in list_times:
current_time = list_times[0]
readings = sc \
    .cassandraTable("cer", "readings") \
    .select("meter_id", "date", "measurement") \
    .where("date <= '{}' AND date >= '{}'".format(current_time, current_time-mk*sample_frequency))\
    .map(lambda x: (x["meter_id"], (x["date"], x["measurement"])))\
    .groupByKey()\
    .mapValues(lambda x: pd.Series(list(i[1] for i in x), index=list(i[0] for i in x)))

model_parameters = sc \
    .cassandraTable("cer", "models") \
    .map(lambda x: (x["meter_id"], np.asanyarray(x["w"])))

data = readings.join(model_parameters)\
    .collect()
#     .map(lambda x: (x[0], np.asanyarray(x[1][0][0].tolist()), x[1][0][1], x[1][1].tolist())).persist()
#
# diff = data.map(lambda x: (x[0], x[1], x[2], x[3], np.dot(x[2], x[1].transpose())-x[3]))
#
# w = diff.map(lambda x: (x[0], x[1], x[2], x[3], (x[2]-x[1] * 2 * x[3] / np.sqrt(i)*lrate), SE + np.square(x[3]))).collect()
# print(i)
# i=i+1
program_end_time = t.time()
print("fin-spark cassandra tests; Time: %d" % (program_end_time - program_start_time))
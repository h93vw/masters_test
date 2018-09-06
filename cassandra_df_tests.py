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

current_time = datetime.datetime(2010, 10, 7, 0, 0)
sample_frequency = datetime.timedelta(minutes=30)
num_tests = 2
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
sqlContext = SQLContext(sc)
model_parametersDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="models", keyspace="cer").load()

lagged_readingsDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="readings", keyspace="cer")\
    .load().filter("date <= '{}' AND date >= '{}'".format(current_time, current_time-mk*sample_frequency)).groupBy("meter_id")\
    .agg(F.collect_list("date"), F.collect_list("measurement"))\
    .rdd.map(lambda x: (x[0]))
    # .rdd.map(lambda x: (x[0], (pd.Series(x[2], index=x[1])))).collect()

model_parametersDF.join(lagged_readingsDF, "meter_id").show()


'''https://stackoverflow.com/questions/33681487/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark'''
print("k")
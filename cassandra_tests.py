from pyspark_cassandra import CassandraSparkContext
from meter_selection import get_meters
from pyspark import SparkContext, SQLContext
from pyspark.sql import functions as F
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
num_of_meters = 30
sample_frequency = datetime.timedelta(minutes=30)
window_size = datetime.timedelta(hours=24)
init_model_params = {}
meter_ids = get_meters()
mk = 3
lrate = 0.75

sc = CassandraSparkContext(appName="PySpark Cassandra Test", master="local[*]")

'''DataFrame Tests'''
sqlContext = SQLContext(sc)
model_parametersDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="models", keyspace="cer").load()
lagged_readingsDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="readings", keyspace="cer")\
    .load().filter("date < '{}' AND date >= '{}'")
    # .format(current_time, current_time-mk*sample_frequency)).groupBy("meter_id")\
    # .agg((F.collect_list("date"), F.collect_list("measurement"))).show()

test = lagged_readingsDF.select('meter_id','date','measurement').rdd.map(lambda x: (x["meter_id"], (x["date"], x["measurement"]))).groupByKey().mapValues(list).collect()

# lagged_readingsDF1 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="readings", keyspace="cer") \
#     .load()
# lagged_readingsDF1.

# model_parametersDF.registerTempTable("models")
# lagged_readingsDF.registerTempTable("readings")
#
# sqlContext.sql("SELECT meter_id, ")


'''Init model parameters'''
# for meter_id in meter_ids:
#     init_model_params[meter_id] = [random.random() for x in range(mk)]
#
# model_parameters1 = sc.parallelize([{
#     "meter_id": k,
#     "w": v
# } for k, v in init_model_params.items()])
# model_parameters1.saveToCassandra("cer", "models")

# lagged_readings = sc \
#     .cassandraTable("cer", "readings") \
#     .select("meter_id", "date", "measurement") \
#     .where("date < '{}' AND date >= '{}'".format(current_time, current_time-mk*sample_frequency))\
#     .map(lambda x: (x["meter_id"], (x["date"], x["measurement"])))\
#     .groupByKey().mapValues(list)
#
#
# model_parameters = sc \
#     .cassandraTable("cer", "models") \
#     .map(lambda x: (x["meter_id"], (x["w"])))
#
# test = lagged_readings.join(model_parameters).collect()
print('test')
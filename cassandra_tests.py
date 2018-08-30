from pyspark_cassandra import CassandraSparkContext
import datetime

# from pyspark import SparkConf
# from cassandra.cluster import Cluster

current_time = datetime.datetime(2010, 10, 7, 0, 0)
num_of_meters = 30
sample_frequency = datetime.timedelta(minutes=30)
window_size = datetime.timedelta(weeks=30)
mk = 3
lrate = 0.75

# cluster = Cluster()
# session = cluster.connect('cer')
#
# rows = session.execute('SELECT meter_id, readings, w FROM test')
# for (meter_id, readings, w) in rows:
#     print('Meter:{}'.format(meter_id))
#     print('Readings:')
#     for key, value in readings.items():
#         print('{}: {}'.format(key, value))
#     print('Model Parameters:')
#     for param in w:
#         print('{}'.format(param))
# print("test")

sc = CassandraSparkContext(appName="PySpark Cassandra Test", master="local[*]")

test = sc \
    .cassandraTable("cer", "readings") \
    .select("meter_id", "date", "measurement") \
    .where("date > '2009-07-17' AND date < '2009-07-18'")\
    .collect()
print('test')
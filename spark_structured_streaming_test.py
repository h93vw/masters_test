

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredStreamingTest") \
    .getOrCreate()

# Read text from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# socketDF.isStreaming()    # Returns True for DataFrames that have streaming sources
#
# socketDF.printSchema()

# Read all the csv files written atomically in a directory
userSchema = StructType().add("meter_id", "integer").add("timecode", "string").add("energy", "double")
csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .schema(userSchema) \
    .csv("data/cer")  # Equivalent to format("csv").load("/path/to/directory")

query = csvDF.writeStream\
    .format("console")\
    .outputMode("update").start()

# csvDF.select("energy").where("meter_id = 1000")
# csvDF.groupBy("timecode").count()

query.awaitTermination()
# from datetime import datetime
# import sys
# import time
#
# import pandas as pd
# import numpy as np
# import pytz
# from pyspark import SparkContext, SQLContext
# from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType
# from sparkts.datetimeindex import uniform, MinuteFrequency
# from sparkts.timeseriesrdd import time_series_rdd_from_observations
# from sparkts.models import ARIMA
#
# from sklearn.metrics import mean_squared_error
# from math import sqrt
#
# import matplotlib.pyplot as plt
#
# # date_time_format = '%Y-%m-%d %H:%M'
# date_time_format = '%Y-%m-%d %H:%M:%S'
#
#
#
# meterids = []
# mapes = []
# forecast_times = []
#
# def lineToRow(line):
#     (meter_id, time, measurement) = line.split(',')
#     # Python 2.x compatible timestamp generation
#     #if meter_id in meterids:
#     dt = datetime.strptime(time, date_time_format).replace(tzinfo=pytz.UTC)
#     return dt, int(meter_id), float(measurement)
#
#
# def loadObservations(sparkContext, sqlContext, path):
#     textFile = sparkContext.textFile(path)
#     rowRdd = textFile.map(lineToRow)
#     schema = StructType([
#         StructField('timestamp', TimestampType(), nullable=True),
#         StructField('meter_id', StringType(), nullable=True),
#         StructField('consumption', DoubleType(), nullable=True),
#     ])
#     return sqlContext.createDataFrame(rowRdd, schema)
#
# if __name__ == "__main__":
#
#     program_start_time = time.time()
#     sc = SparkContext(appName="ARIMA_Forecast", master="local[*]")
#     sqlContext = SQLContext(sc)
#
#     # data = loadObservations(sc, sqlContext, "..\\data\\cer\\certest2.csv")
#     data = loadObservations(sc, sqlContext, "..\\data\\cer\\File1.csv")
#     data.dropna()
#
#     # Create an half hour DateTimeIndex over August and September 2015
#     freq = MinuteFrequency(30, sc)
#
#     starttime = pd.Timestamp('2010-06-24 00:30:00')
#     endtime = pd.Timestamp('2010-07-15 0:30:00')
#     dtIndex = uniform(start=starttime, end=endtime, freq=freq, sc=sc)
#
#     # dtIndex = uniform(start='2016-08-29T00:00-00:00', end='2016-09-19T00:00-00:00', freq=freq, sc=sc)
#
#     # Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
#     readingsTSRDD = time_series_rdd_from_observations(dtIndex, data, "timestamp", "meter_id", "consumption")
#     readingsTSRDD.map(lambda x: (x[0], tuple(np.array(_) for _ in zip(*x[1:]))))
#     collected_readings = readingsTSRDD.sortByKey().collect()
#     data_load_time = time.time() - program_start_time
#     for meterid in collected_readings:
#
#         ts = meterid[1]
#         tstrain = ts[0:-48]
#         observed = ts[-48:]
#
#         try:
#
#             individual_start = time.time()
#             model = ARIMA.autofit(tstrain, maxp=3, maxd=3, maxq=3, sc=sc)
#             forecast = model.forecast(ts, 48)
#             forecast1 = forecast[-48:]
#
#             # plt.plot(observed)
#             # plt.plot(forecast1)4
#             # plt.ylabel('Electricity (kWh)')
#             # plt.show()
#
#             rmse = sqrt(mean_squared_error(observed, forecast1))
#             mae = np.mean(np.abs(observed - forecast1))
#             # mbe =
#             if 0 in observed:
#                 mape = np.nan
#             else:
#                 mape = np.mean(np.abs((observed - forecast1) / observed)) * 100
#
#             # print('Meterid: {}    RMSE:{}    MAE:{}    MAPE:{}'.format(meterid[0], rmse, mae, mape))
#             forecast_times.append(time.time()-individual_start)
#             meterids.append(meterid[0])
#             mapes.append(mape)
#
#         except:
#             print('Error on meter {}'.format(meterid[0]))
#
#     program_end_time = time.time()
#
#     print('Finished \n\nTotal Execution Time: {}'.format((program_end_time-program_start_time)))
#     print('Average MAPE: {}'.format(np.nanmean(mapes)))
#
#     print("fin")
#     sys.exit(0)

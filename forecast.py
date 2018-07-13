import datetime
import time as t

from meter_selection import get_meters
# from parameter_selection import get_parameters_list
# from parameter_selection import get_parameters_csv

import numpy as np
import pandas as pd
import psycopg2
import statsmodels.api as sm
import itertools
import warnings

# meterids = [1000, 1001, 1002]
# orders = ["((1, 0, 2), (1, 1, 1, 48))", "((2, 1, 1), (0, 0, 0, 48))", "((2, 0, 1), (1, 0, 1, 48))"]


def forecast_SARIMA_fixed_order(data_in, pdq=(1, 0, 2), seasonal_pdq=(0, 1, 1, 48)):

    mapes = {}
    times = {}

    program_start_time = t.time()

    for row in data_in:
        individual_start_time = t.time()
        meter_id = row[0]
        endog = row[1]
        observed = row[2]
        if not(len(observed.index)):
            raise ValueError('please check forecast_check on data_gather')
        try:
            print("Begin - Building model for meter: %d" % meter_id)
            warnings.filterwarnings("ignore")  # specify to ignore warning messages

            mod = sm.tsa.statespace.SARIMAX(endog,
                                            order=pdq,
                                            seasonal_order=seasonal_pdq,
                                            enforce_stationarity=False,
                                            enforce_invertibility=False)

            results = mod.fit(disp=0)
            forecast = results.get_forecast(steps=48)

            mape = np.mean(np.abs((observed - forecast.predicted_mean.values) / observed)) * 100
            print('MAPE: {}'.format(mape))

            individual_end_time = t.time()

            mapes[meter_id] = mape
            times[meter_id] = (individual_end_time - individual_start_time)

        except:
            print("Skipping meter: %d" % meter_id)
            continue

    program_end_time = t.time()
    print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))
    return mapes, times


def forecast_ARIMAX_fixed_order(data_in, pdq=(0, 1, 2)):

    mapes = {}
    times = {}

    program_start_time = t.time()

    for row in data_in:
        individual_start_time = t.time()
        meter_id = row[0]
        endog = row[1]
        observed = row[2]
        exog = row[3]
        observed_exog = row[4]

        if not(len(observed.index)):
            raise ValueError('please check forecast_check on data_gather')
        if not(len(exog.index)):
            raise ValueError('please check exog_check on data_gather')
        try:
            print("Begin - Building model for meter: %d" % meter_id)
            warnings.filterwarnings("ignore")  # specify to ignore warning messages

            mod = sm.tsa.statespace.SARIMAX(endog, exog,
                                            order=pdq,
                                            enforce_stationarity=False,
                                            enforce_invertibility=False)

            results = mod.fit(disp=0)
            forecast = results.get_forecast(steps=48, exog=observed_exog)

            mape = np.mean(np.abs((observed - forecast.predicted_mean.values) / observed)) * 100
            print('MAPE: {}'.format(mape))

            individual_end_time = t.time()

            mapes[meter_id] = mape
            times[meter_id] = (individual_end_time - individual_start_time)

        except:
            print("Skipping meter: %d" % meter_id)
            continue

    program_end_time = t.time()
    print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))
    return mapes, times


# def forecast_SARIMA_csv_order():
#     times = []
#     mapes = []
#     csv_input = open('..\\ARIMATest\\data\\cer\\arima_params.csv', "r")
#     params = get_parameters_csv(csv_input)
#     csv_input.close()
#     program_start_time = t.time()
#     meterids = []
#
#     #  date_time_format = '%Y-%m-%d %H:%M:%S.%f'  # .%f needed for java generated csv file
#
#     for param in params:
#         try:
#             individual_start_time = t.time()
#             meterid = int(param[0])
#             meterids.append(meterid)
#             pdq = param[1][0]
#             seasonal_pdq = param[1][1]
#             conn = psycopg2.connect("host='localhost' port='5432' dbname='cer' user='postgres' password='welcome1'")
#
#             # create a cursor
#             cur = conn.cursor()
#
#             cur.execute("SELECT date, measurement FROM readings WHERE meterid = %d ORDER BY date" % meterid)
#             rows = cur.fetchall()
#             timestamps = [row[0] for row in rows]
#             agg = [row[1] for row in rows]
#
#             # window_size = datetime.timedelta(weeks=1)
#             # one_year_data = times[0] + datetime.timedelta(days=365)
#             # end_time = one_year_data
#             # start_time = end_time - window_size
#
#             start_time = timestamps[0]
#             end_time = timestamps[-1] - datetime.timedelta(hours=24)
#
#             # move forward a year
#             current_readings_index = timestamps.index(timestamps[0] + datetime.timedelta(days=365))
#
#             window_size = datetime.timedelta(weeks=3)
#             window_start = timestamps[current_readings_index]
#             window_end = window_start + window_size
#             time_window = timestamps[current_readings_index:-1]
#
#             endog = []
#
#             for timestamp in time_window:
#                 current_index = timestamps.index(timestamp)
#
#                 endog.append(agg[current_index])
#
#             endog = pd.Series(endog, index=time_window, dtype='Float64')[window_start:window_end]
#
#             observed_window = timestamps[current_readings_index + 1: current_readings_index + 49]
#
#             observed = []
#
#             for timestamp in observed_window:
#                 current_index = timestamps.index(timestamp)
#
#                 observed.append(agg[current_index])
#
#             print("Data fetched building and evaluating models")
#             warnings.filterwarnings("ignore")  # specify to ignore warning messages
#
#             try:
#                 mod = sm.tsa.statespace.SARIMAX(endog,
#                                                 order=pdq,
#                                                 seasonal_order=seasonal_pdq,
#                                                 enforce_stationarity=False,
#                                                 enforce_invertibility=False)
#
#                 results = mod.fit(disp=0)
#                 forecast = results.get_forecast(steps=48)
#                 mape = np.mean(np.abs((observed - forecast.predicted_mean.values) / observed)) * 100
#                 print('MAPE: {}'.format(mape))
#                 mapes.append(mape)
#
#                 # print('ARIMA{} - AIC:{}'.format(param, results.aic))
#                 individual_end_time = t.time()
#                 times.append(individual_end_time - individual_start_time)
#
#             except:
#                 continue
#
#         except:
#             print("Skipping meter: %d" % meterid)
#             continue
#
#     conn.close()
#     program_end_time = t.time()
#     print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))
#     return (meterids, mapes, times)
#
#
# def forecast_ARIMAX_csv_orders(meterids, orders):
#     times = []
#     mapes = []
#
#     program_start_time = t.time()
#
#     # date_time_format = '%Y-%m-%d %H:%M:%S.%f'  # .%f needed for java generated csv file
#
#     for mid in meterids:
#         try:
#             individual_start_time = t.time()
#             meterid = int(mid)
#             order = orders[meterids.index(mid)]
#             conn = psycopg2.connect("host='localhost' port='5432' dbname='cer' user='postgres' password='welcome1'")
#
#             # create a cursor
#             cur = conn.cursor()
#
#             cur.execute("SELECT date, measurement FROM readings WHERE meterid = %d ORDER BY date" % meterid)
#             rows = cur.fetchall()
#             timestamps = [row[0] for row in rows]
#             agg = [row[1] for row in rows]
#
#             # window_size = datetime.timedelta(weeks=1)
#             # one_year_data = times[0] + datetime.timedelta(days=365)
#             # end_time = one_year_data
#             # start_time = end_time - window_size
#
#             start_time = timestamps[0]
#             end_time = timestamps[-1] - datetime.timedelta(hours=24)
#
#             # move forward a year
#             current_readings_index = timestamps.index(timestamps[0] + datetime.timedelta(days=365))
#
#             window_size = datetime.timedelta(weeks=3)
#             window_start = timestamps[current_readings_index]
#             window_end = window_start + window_size
#             time_window = timestamps[current_readings_index:-1]
#
#             endog = []
#             # exog = []
#             prevrd = []
#             prevhr = []
#             prevwk = []
#             prevyr = []
#
#             for timestamp in time_window:
#                 current_index = timestamps.index(timestamp)
#
#                 prev_reading_index = current_index - 1
#                 prev_hour_index = timestamps.index(timestamp - datetime.timedelta(hours=1))
#                 prev_day_index = timestamps.index(timestamp - datetime.timedelta(days=1))
#                 prev_week_index = timestamps.index(timestamp - datetime.timedelta(weeks=1))
#                 # prev_month_index = timestamps.index(time - datetime.timedelta(weeks=4))
#                 prev_year_index = timestamps.index(timestamp - datetime.timedelta(days=365))
#
#                 endog.append(agg[current_index])
#
#                 prevrd.append(agg[prev_reading_index])
#                 prevhr.append(agg[prev_hour_index])
#                 prevwk.append(agg[prev_week_index])
#                 prevyr.append(agg[prev_year_index])
#
#             endog = pd.Series(endog, index=time_window, dtype='Float64')[window_start:window_end]
#
#             exog = pd.DataFrame({'prevRd': prevrd, 'prevHr': prevhr,
#                                  'prevWk': prevwk, 'prevYr': prevyr
#                                  }, dtype='Float64', index=time_window)[window_start:window_end]
#
#             observed_window = timestamps[current_readings_index + 1: current_readings_index + 49]
#
#             observed = []
#             prevrd = []
#             prevhr = []
#             prevwk = []
#             prevyr = []
#
#             for timestamp in observed_window:
#                 current_index = timestamps.index(timestamp)
#
#                 prev_reading_index = current_index - 1
#                 prev_hour_index = timestamps.index(timestamp - datetime.timedelta(hours=1))
#                 prev_day_index = timestamps.index(timestamp - datetime.timedelta(days=1))
#                 prev_week_index = timestamps.index(timestamp - datetime.timedelta(weeks=1))
#                 # prev_month_index = timestamps.index(time - datetime.timedelta(weeks=4))
#                 prev_year_index = timestamps.index(timestamp - datetime.timedelta(days=365))
#
#                 observed.append(agg[current_index])
#
#                 prevrd.append(agg[prev_reading_index])
#                 prevhr.append(agg[prev_hour_index])
#                 prevwk.append(agg[prev_week_index])
#                 prevyr.append(agg[prev_year_index])
#
#             observed_exog = pd.DataFrame({'prevRd': prevrd, 'prevHr': prevhr,
#                                           'prevWk': prevwk, 'prevYr': prevyr
#                                           }, dtype='Float64', index=observed_window)
#
#
#
#             print("Data fetched building and evaluating models")
#             warnings.filterwarnings("ignore")  # specify to ignore warning messages
#
#             try:
#                 mod = sm.tsa.statespace.SARIMAX(endog,
#                                                 exog,
#                                                 order=order,
#                                                 # seasonal_order=param_seasonal,
#                                                 enforce_stationarity=False,
#                                                 enforce_invertibility=False)
#
#                 results = mod.fit(disp=0)
#                 forecast = results.get_forecast(steps=48, exog=observed_exog)
#                 mape = np.mean(np.abs((observed - forecast.predicted_mean.values) / observed)) * 100
#                 print('MAPE: {}'.format(mape))
#                 mapes.append(mape)
#
#                 # print('ARIMA{} - AIC:{}'.format(param, results.aic))
#                 individual_end_time = t.time()
#                 times.append(individual_end_time - individual_start_time)
#
#             except:
#                 continue
#
#         except:
#             print("Skipping meter: %d" % meterid)
#             continue
#
#     conn.close()
#     program_end_time = t.time()
#     print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))
#     return (meterids, mapes, times)






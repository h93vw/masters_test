import datetime
import time as t

from meter_selection import get_meters
# from parameter_selection import get_parameters_list
# from parameter_selection import get_parameters_csv

import numpy as np
import statsmodels.api as sm
import matplotlib.pylab as plt
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

    # plot
    ax = observed.plot(label='observed')
    forecast.predicted_mean.plot(ax=ax, label='Forecast')
    ax.set_xlabel('Date')
    ax.set_ylabel('Demand')

    plt.legend()
    plt.show()

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
    # plot
    ax = observed.plot(label='observed')
    forecast.predicted_mean.plot(ax=ax, label='Forecast')
    ax.set_xlabel('Date')
    ax.set_ylabel('Demand')

    plt.legend()
    plt.show()
    print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))
    return mapes, times


def forecast_clusters(data_in, cluster_orders):
    mapes = {}
    times = {}

    program_start_time = t.time()

    for row in data_in:
        individual_start_time = t.time()
        meter_id = row[0]
        endog = row[1]
        observed = row[2]
        pdq, seasonal_pdq = cluster_orders[meter_id]

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








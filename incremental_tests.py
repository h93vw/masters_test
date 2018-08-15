from data_gather import *
from order_estimation import *
from forecast import *
from clusterer import *


import matplotlib.pylab as plt
import time as t
import datetime

# csv_ARIMAX_orders = open('..\\masters_test\\data\\orders_ARIMAX.csv', "r")

current_time = datetime.datetime(2010, 10, 7, 0, 0)
num_of_meters = 1
sample_frequency = datetime.timedelta(minutes=30)
window_size = datetime.timedelta(weeks=12)
fixed_pdq = (0, 1, 2)
seasonal_pdq = (0, 1, 1, 48)
count = 0


def update_time(current_time, sample_freq=datetime.timedelta(minutes=30)):
    current_time = current_time + sample_freq
    return current_time

def init_lookback_windows(current_time, num_of_meters=1, window_size=datetime.timedelta(weeks=12)):
    lookback_windows = gather_cer_data_window(current_time, num_of_meters, window_size=window_size)
    return lookback_windows


def update_lookback_windows(current_time, lookback_windows, window_size=datetime.timedelta(weeks=12)):
    old_time = current_time-window_size
    for key, value in lookback_windows.items():
        new_value = gather_cer_data_single(current_time, key)
        lookback_windows[key] = value.drop(old_time).append(new_value)
    return lookback_windows

def fit_build_models(lookback_windows, pdq=(0, 1, 2), seasonal_pdq=(0, 0, 0, 48)):
    models = {}
    for key, value in lookback_windows.items():
        build_model = sm.tsa.statespace.SARIMAX(value,
                                        order=pdq,
                                        seasonal_order=seasonal_pdq,
                                        enforce_stationarity=False,
                                        enforce_invertibility=False)
        fit_model = build_model.fit(disp=0)
        models[key] = fit_model
    return models


def make_predictions(models, predictions=None, forecast_horizon=datetime.timedelta(days=1), sample_freq=datetime.timedelta(minutes=30)):
    number_steps = int(forecast_horizon.total_seconds()/sample_freq.total_seconds())
    for key, value in models.items():
        prediction = value.get_forecast(steps=number_steps)
        if predictions == None:
            predictions = {}
            prediction_list = []
        else:
            prediction_list = predictions[key]
        prediction_list.append(prediction)
        predictions[key] = prediction_list
        return predictions


def verify_forecast(lookback_window, predictions, mapes=None):
    if mapes == None:
        mapes = {}
    data_align_check = False
    for key, value in lookback_windows.items():
        lookback_window_start = pd.Timestamp(lookback_window[key].index.values[0])
        lookback_window_end = pd.Timestamp(lookback_window[key].index.values[-1])
        prediction_window_start = pd.Timestamp(predictions[key][0].row_labels[0])
        prediction_window_end = pd.Timestamp(predictions[key][0].row_labels[-1])
        if prediction_window_start == lookback_window_start:
            print("success")
            if prediction_window_end == lookback_window_end:
                if len(lookback_window) == len(predictions[key][0]):
                    print("Super success")
                    mape = np.nanmean(np.abs((lookback_window[key][0].values - predictions[key][0].predicted_mean.values) / lookback_window[key][0].values)) * 100
                    mapes[key] = mape
        else:
            print("Predictions and lookback window not aligned")
    return mapes


print("Begin Initialization")
program_start_time = t.time()

lookback_windows = init_lookback_windows(current_time, num_of_meters, window_size=window_size)
models = fit_build_models(lookback_windows, fixed_pdq, seasonal_pdq)
predictions = make_predictions(models)

initialization_end_time = t.time()
print("Finished Initialization - {}s \nStarting update".format(initialization_end_time-program_start_time))
update_start_time = t.time()
mapes = verify_forecast(lookback_windows, predictions)
while not mapes:
    count += 1
    print("update_count = {}".format(count))
    current_time = update_time(current_time)
    lookback_windows = update_lookback_windows(current_time, lookback_windows)
    models = fit_build_models(lookback_windows, fixed_pdq, seasonal_pdq)
    predictions = make_predictions(models, predictions)
    mapes = verify_forecast(lookback_windows, predictions, mapes)
    if count == 48:
        print("test")

update_end_time = t.time()
print("Finished Update - {}s".format(update_end_time-update_start_time))

# observed =
#
# for item in data:
#     mod = sm.tsa.statespace.SARIMAX(item,
#                                     order=pdq,
#                                     enforce_stationarity=False,
#                                     enforce_invertibility=False)
#     results = mod.fit(disp=0)
#     params = results.summary()
#     print(params)
#     forecast = results.get_forecast(steps=48)
#
#     mape = np.mean(np.abs((observed - forecast.predicted_mean.values) / observed)) * 100
#
#
# mapes_list = list(mapes.values())
# cleanmapes = [x for x in mapes_list if (x != np.inf)]
# plt.plot(list(mapes.values()))
# plt.show()
# print("fin; Average MAPE per meter: %f" % (sum(cleanmapes) / len(cleanmapes)))

program_end_time = t.time()
print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))

# plt.plot(lookback_windows[1000])
# plt.plot(predictions[1000].predicted_mean)
# plt.show()
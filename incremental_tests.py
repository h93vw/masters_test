from data_gather import *
from order_estimation import *
from forecast import *
from clusterer import *


import matplotlib.pylab as plt
import time as t
import datetime

# csv_ARIMAX_orders = open('..\\masters_test\\data\\orders_ARIMAX.csv', "r")

current_time = datetime.datetime(2016, 12, 12, 0, 0) #(2010, 10, 7, 0, 0)
num_of_meters = 1
sample_frequency = datetime.timedelta(minutes=30)
window_size = datetime.timedelta(weeks=12)
fixed_pdq = (0, 1, 2)
fixed_seasonal_pdq = (0, 1, 1, 48)
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


# def fit_build_models(lookback_windows, pdq=(0, 1, 2), seasonal_pdq=(0, 0, 0, 48)):
#     models = {}
#     for key, value in lookback_windows.items():
#         build_model = sm.tsa.statespace.SARIMAX(value,
#                                                 order=pdq,
#                                                 seasonal_order=seasonal_pdq,
#                                                 enforce_stationarity=False,
#                                                 enforce_invertibility=False)
#
#         fit_model = build_model.fit(disp=0)
#         params = fit_model.params
#         models[key] = fit_model
#     return models

def fit_build_models(lookback_windows, models=None, params=None, pdq=(0, 1, 2), seasonal_pdq=(0, 0, 0, 48)):
    if models is None:
        models = {}
    for key, value in lookback_windows.items():
        if params is None:
            params = {}
            params_list = None
            initial_params = None
        elif key not in params:
            params_list = None
            initial_params = None
        else:
            params_list = params[key]
            if isinstance(params_list, pd.DataFrame):
                loc = value.index[-2]
                initial_params = params_list[loc]
            else:
                initial_params = params_list
        build_model = sm.tsa.statespace.SARIMAX(value,
                                                order=pdq,
                                                seasonal_order=seasonal_pdq,
                                                enforce_stationarity=False,
                                                enforce_invertibility=False)

        # test = len(params_list)
        # if len(params_list) > 1 :
        #     initial_param = params_list[-1]

        fit_model = build_model.fit(disp=False, start_params=initial_params)
        if params_list is not None:
            params_list = pd.concat([params_list, fit_model.params.rename(value.index[-1])], axis=1)
            params_list.sort_index(axis=1, inplace=True)
        else:
            params_list = fit_model.params.rename(value.index[-1])
        models[key] = fit_model
        params[key] = params_list
    return models, params


def make_predictions(models, predictions=None, forecast_horizon=datetime.timedelta(days=1), sample_freq=datetime.timedelta(minutes=30)):
    number_steps = int(forecast_horizon.total_seconds()/sample_freq.total_seconds())
    for key, value in models.items():
        if predictions == None:
            predictions = {}
            prediction_list = []
        else:
            prediction_list = predictions[key]
        prediction = value.get_forecast(steps=number_steps)
        prediction_series = pd.Series(prediction.predicted_mean.values, index=prediction.row_labels, dtype="Float64")
        prediction_list.append(prediction_series)
        predictions[key] = prediction_list
        return predictions


def verify_forecast(lookback_window, predictions, mapes=None):
    if mapes == None:
        mapes = {}
    data_align_check = False
    for key, value in lookback_windows.items():
        prediction_list = predictions[key]
        current_prediction = prediction_list[0]
        prediction_window_start =current_prediction.index.values[0]
        prediction_window_end = current_prediction.index.values[-1]

        observed_window = lookback_window[key][prediction_window_start:prediction_window_end]
        if observed_window.any():
            if observed_window.index.values[0] == prediction_window_start:
                if observed_window.index.values[-1] == prediction_window_end:
                    if len(observed_window) == len(current_prediction):
                        mape = np.nanmean(np.abs((observed_window.values - current_prediction.values) / observed_window.values)) * 100
                        if mapes:
                            mape_list = mapes[key]
                        else:
                            mape_list = []
                        mape_list.append(mape)
                        mapes[key] = mape_list
                        prediction_list.remove(current_prediction)
                        predictions[key] = prediction_list
                        print("Success - Mape: {} - Avg {}".format(mape, np.nanmean(mape_list)))
                else:
                    print("Predictions and lookback window not aligned")
    return mapes, predictions


print("Begin Initialization")
program_start_time = t.time()

lookback_windows = init_lookback_windows(current_time, num_of_meters, window_size=window_size)
models, params = fit_build_models(lookback_windows, models=None, params=None, pdq=fixed_pdq, seasonal_pdq=fixed_seasonal_pdq)
predictions = make_predictions(models)

initialization0_end_time = t.time()
print("Finished lookback_window Initialization - {}s \nStarting update".format(initialization0_end_time-program_start_time))
initialization1_start_time = t.time()
mapes, predictions = verify_forecast(lookback_windows, predictions)
while not mapes:
    count += 1
    print("update_count = {}".format(count))
    current_time = update_time(current_time)
    lookback_windows = update_lookback_windows(current_time, lookback_windows)
    models, params = fit_build_models(lookback_windows, models, params, pdq=fixed_pdq, seasonal_pdq=fixed_seasonal_pdq) #, seasonal_pdq)
    predictions = make_predictions(models, predictions)
    mapes, predictions = verify_forecast(lookback_windows, predictions, mapes)

initialization1_end_time = t.time()
print("Finished predictions Initialization - {}s".format(initialization1_end_time-initialization1_start_time))
while count < 10:
    count += 1
    print("update_count = {}".format(count))
    current_time = update_time(current_time)
    lookback_windows = update_lookback_windows(current_time, lookback_windows)
    models, params = fit_build_models(lookback_windows, models, params, fixed_pdq, fixed_seasonal_pdq) #, seasonal_pdq)
    predictions = make_predictions(models, predictions)
    mapes, predictions = verify_forecast(lookback_windows, predictions, mapes)

program_end_time = t.time()
print("fin-ARIMAX_forecast; Time: %d" % (program_end_time - program_start_time))

# plt.plot(lookback_windows[1000])
# plt.plot(predictions[1000].predicted_mean)
# plt.show()
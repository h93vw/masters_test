import access_sql
import datetime

import time as t
import pandas as pd
import numpy as np

from meter_selection import get_meters

import matplotlib.pylab as plt

# date_time_format = '%Y-%m-%d %H:%M:%S.%f'  # .%f needed for java generated csv file


def gather_cer_data(number_of_meters=4621, start=1000, window_size=datetime.timedelta(weeks=3), exog_check=False, forecast_check=False):

    endogs = []
    observeds = []
    exogs = []
    observed_exogs =[]
    data = []

    print("Begin - Data fetch")
    program_start_time = t.time()

    meter_ids = get_meters(number_of_meters, start=start)

    conn = access_sql.connect_postgresql()

    for meter_id in meter_ids:
        print('{} gather - Fetching data from meter {}'.format(meter_ids.index(meter_id), meter_id))

        query = "SELECT date, measurement FROM readings WHERE meterid = %d ORDER BY date" % meter_id

        try:

            endog = []
            # for exog
            prevrd = []
            prevdy = []
            prevwk = []
            prevyr = []
            exog = []
            observed = []
            # for observed exog
            prevrdo = []
            prevdyo = []
            prevwko = []
            prevyro = []
            observed_exog = []

            rows = access_sql.access_postgresql(conn, query)

            timestamps = [row[0] for row in rows]
            agg = [row[1] for row in rows]

            start_time = timestamps[0]
            end_time = timestamps[-1] - datetime.timedelta(hours=24)

            # move forward a year
            current_readings_index = timestamps.index(start_time + datetime.timedelta(days=365))

            window_start = timestamps[current_readings_index]
            window_end = window_start + window_size

            if window_end > end_time:
                raise ValueError('Window size too large for data given')

            forecast_start = window_end
            forecast_end = forecast_start + datetime.timedelta(hours=24)

            time_window = timestamps[timestamps.index(window_start):timestamps.index(window_end)]
            observed_window = timestamps[timestamps.index(forecast_start):timestamps.index(forecast_end)]

            for timestamp in time_window:
                current_index = timestamps.index(timestamp)
                endog.append(agg[current_index])

                if exog_check:

                    prev_reading_index = current_index - 1
                    # prev_hour_index = timestamps.index(timestamp - datetime.timedelta(hours=1))
                    prev_day_index = timestamps.index(timestamp - datetime.timedelta(days=1))
                    prev_week_index = timestamps.index(timestamp - datetime.timedelta(weeks=1))
                    # prev_month_index = timestamps.index(time - datetime.timedelta(weeks=4))
                    prev_year_index = timestamps.index(timestamp - datetime.timedelta(days=365))

                    prevrd.append(agg[prev_reading_index])
                    prevdy.append(agg[prev_day_index])
                    prevwk.append(agg[prev_week_index])
                    prevyr.append(agg[prev_year_index])

            endog = pd.Series(endog, index=time_window, dtype='Float64')[window_start:window_end]

            if exog_check:
                exog = pd.DataFrame({'prevRd': prevrd, 'prevDy': prevdy,
                                     'prevWk': prevwk, 'prevYr': prevyr
                                     }, dtype='Float64', index=time_window)[window_start:window_end]

            if forecast_check:
                for timestamp in observed_window:
                    current_index = timestamps.index(timestamp)
                    observed.append(agg[current_index])

                    if exog_check:
                        prev_reading_indexo = current_index - 1
                        # prev_hour_indexo = timestamps.index(timestamp - datetime.timedelta(hours=1))
                        prev_day_indexo = timestamps.index(timestamp - datetime.timedelta(days=1))
                        prev_week_indexo = timestamps.index(timestamp - datetime.timedelta(weeks=1))
                        # prev_month_indexo = timestamps.index(time - datetime.timedelta(weeks=4))
                        prev_year_indexo = timestamps.index(timestamp - datetime.timedelta(days=365))

                        prevrdo.append(agg[prev_reading_indexo])
                        prevdyo.append(agg[prev_day_indexo])
                        prevwko.append(agg[prev_week_indexo])
                        prevyro.append(agg[prev_year_indexo])

                observed = pd.Series(observed, index=observed_window, dtype='Float64')[forecast_start:forecast_end]

                if exog_check:
                    observed_exog = pd.DataFrame({'prevRd': prevrdo, 'prevDy': prevdyo,
                                                  'prevWk': prevwko, 'prevYr': prevyro
                                                  }, dtype='Float64', index=observed_window)[forecast_start:forecast_end]

                if len(observed) != 48:
                    raise ValueError('missing some observed values')

                if np.isnan(observed).any():
                    raise ValueError('observed contained NaN value')

        except:
            print("Error: Skipping meter: %d" % meter_id)
            meter_ids.remove(meter_id)
            continue

        endogs.append(endog)

        if forecast_check:
            observeds.append(observed)
        if exog_check:
            exogs.append(exog)
            if forecast_check:
                observed_exogs.append(observed_exog)

        data.append([meter_id, endog, observed, exog, observed_exog, time_window, observed_window])

    program_end_time = t.time()
    print("fin-Data_fetch; Time: %d " % (program_end_time - program_start_time))
    access_sql.disconnect_postgresql(conn)
    return data


def data_check(data):
    observed_window_check = data[0][6]
    observed_start = observed_window_check[0]
    print(observed_start)
    forecast_length = len(observed_window_check)
    print(forecast_length)
    check = True

    for item in data:
        meter_id = item[0]
        observed_window = item[6]
        print("checking meter %d" % meter_id)
        if observed_window[0] != observed_start:
            print("observed start not aligned for meter %d" % meter_id)
            check = False
        if len(observed_window) != forecast_length:
            print("Dropped readings in observed for meter %d" % meter_id)
            check = False

    if not check:
        print("Data Check Failed")
    else:
        print("Data Check passed")
    return check


# data = gather_cer_data(1, 1060, forecast_check=True)
# data_check(data)
# for row in data:
#     plt.plot(row[1])
#     plt.plot(row[2])
#     plt.show()
#
# print("fin")

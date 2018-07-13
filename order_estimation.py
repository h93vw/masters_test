import itertools
import warnings

import time as t
import statsmodels.api as sm

date_time_format = '%Y-%m-%d %H:%M:%S.%f'  # .%f needed for java generated csv file


def estimate_SARIMAmodelorder(data_in):

    orders = {}
    times = {}

    print("Begin - Estimating SARIMA orders")
    program_start_time = t.time()

    for row in data_in:
        try:
            aic = []
            individual_orders = []

            meter_id = row[0]
            endog = row[1]
            print('*** {} ***'.format(meter_id))
            individual_start_time = t.time()

            # Define the p, d and q parameters to take any value between 0 and 2
            p = d = q = range(0, 3)
            # seasonal order
            sp = sq = sd = range(0, 2)

            # Generate all different combinations of p, q and q triplets
            pdq = list(itertools.product(p, d, q))

            # Generate all different combinations of seasonal p, q and q triplets
            seasonal_pdq = [(x[0], x[1], x[2], 48) for x in list(itertools.product(sp, sd, sq))]

            warnings.filterwarnings("ignore")  # specify to ignore warning messages
            i = 1
            for param_seasonal in seasonal_pdq:
                for param in pdq:
                    try:
                        mod = sm.tsa.statespace.SARIMAX(endog,
                                                        # exog,
                                                        order=param,
                                                        seasonal_order=param_seasonal,
                                                        enforce_stationarity=False,
                                                        enforce_invertibility=False)

                        results = mod.fit(disp=0)

                        print('{} - ARIMA{}x{} - AIC:{}'.format(i, param, param_seasonal, results.aic))
                        i += 1
                        aic.append(results.aic)
                        individual_orders.append([param, param_seasonal])

                    except:
                        continue
                index = aic.index(min(aic))
                best_fit_param = individual_orders[index]
                individual_end_time = t.time()
                individual_time = individual_end_time - individual_start_time

                orders[meter_id] = best_fit_param
                times[meter_id] = individual_time

        except:
            print("Skipping meter: %d" % meter_id)
            continue

    program_end_time = t.time()
    print("fin-estimate_SARIMA_order; Time: %d" % (program_end_time - program_start_time))
    return orders, times


def estimate_ARIMAXmodelorder(data_in):
    orders = {}
    times = {}

    print("Begin - Estimating SARIMA orders")
    program_start_time = t.time()

    for row in data_in:
        try:
            aic = []
            individual_orders = []

            meter_id = row[0]
            endog = row[1]
            exog = row[3]
            if not(len(exog.index)):
                raise ValueError('please check exog_check on data_gather')
            print('*** {} ***'.format(meter_id))
            individual_start_time = t.time()

            # Define the p, d and q parameters to take any value between 0 and 2
            p = d = q = range(0, 3)
            # seasonal order
            # sp = sq = sd = range(0, 2)

            # Generate all different combinations of p, q and q triplets
            pdq = list(itertools.product(p, d, q))

            # Generate all different combinations of seasonal p, q and q triplets
            # seasonal_pdq = [(x[0], x[1], x[2], 48) for x in list(itertools.product(sp, sd, sq))]

            warnings.filterwarnings("ignore")  # specify to ignore warning messages
            i = 1
            # for param_seasonal in seasonal_pdq:
            for param in pdq:
                try:
                    mod = sm.tsa.statespace.SARIMAX(endog,
                                                    exog,
                                                    order=param,
                                                    # seasonal_order=param_seasonal,
                                                    enforce_stationarity=False,
                                                    enforce_invertibility=False)

                    results = mod.fit(disp=0)

                    print('{} - ARIMA{} - AIC:{}'.format(i, param, results.aic))
                    i += 1
                    aic.append(results.aic)
                    individual_orders.append(param)

                except:
                    continue
            index = aic.index(min(aic))
            best_fit_param = individual_orders[index]
            individual_end_time = t.time()
            individual_time = individual_end_time - individual_start_time

            orders[meter_id] = best_fit_param
            times[meter_id] = individual_time
        except:
            print("Skipping meter: %d" % meter_id)
            continue

    program_end_time = t.time()
    print("fin-estimate_ARIMAX_order; Time: %d" % (program_end_time - program_start_time))
    return orders, times



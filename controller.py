from data_gather import *
from order_estimation import *
from forecast import *
from import_csv import *

import matplotlib.pylab as plt


num_of_meters = 100
window_size = window_size=datetime.timedelta(weeks=24)
# csv_ARIMAX_orders = open('..\\masters_test\\data\\orders_ARIMAX%d.csv' % num_of_meters, "r")
# csv_SARIMA_orders = open('..\\masters_test\\data\\orders_SARIMA%d.csv' % num_of_meters, "r")

data = gather_cer_data(num_of_meters, window_size=window_size, exog_check=True, forecast_check=True)
# data_check(data)

'''Cluster'''

'''SARIMA'''
# orders, order_estimation_t = estimate_SARIMAmodelorder(data)
# mapes, forecast_t = forecast_SARIMA_fixed_order(data)
# csv_SARIMA_orders = open('..\\masters_test\\data\\orders_SARIMA%d.csv' % num_of_meters, "w+")
# write_csv_file(csv_SARIMA_orders, orders, data_dict=True)

'''ARIMAX'''
orders, order_estimation_t = estimate_ARIMAXmodelorder(data)
# mapes, forecast_t = forecast_ARIMAX_fixed_order(data)
mapes, forecast_t = forecast_ARIMAX(data, orders)
# write_csv_file(csv_ARIMAX_orders, orders, data_dict=True)


mapes_list = list(mapes.values())
cleanmapes = [x for x in mapes_list if (x != np.inf)]
print("fin; Average MAPE per meter: %f" % (np.nanmean(cleanmapes)))
print("fin")

plt.plot(list(mapes.values()))
plt.show()

"""
TODO:
reate evaluations - accuracy (mape, etc) and time measurements
remove inf forcast mapes - meter 1060?
meter_selection/import_csv combine & update? 
"""

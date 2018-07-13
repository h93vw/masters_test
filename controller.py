from data_gather import *
from order_estimation import *
from forecast import *
from import_csv import *

import matplotlib.pylab as plt

csv_ARIMAX_orders = open('..\\masters_test\\data\\orders_ARIMAX.csv', "w+")
csv_SARIMA_orders = open('..\\masters_test\\data\\orders_SARIMA.csv', "w+")
num_of_meters = 100

data = gather_cer_data(num_of_meters)
# data_check(data)

'''Cluster'''


'''SARIMA'''
orders, order_estimation_t = estimate_SARIMAmodelorder(data)
# mapes, forecast_t = forecast_SARIMA_fixed_order(data)
write_csv_file(csv_ARIMAX_orders, orders, data_dict=True)

'''ARIMAX'''
# orders, order_estimation_t = estimate_ARIMAXmodelorder(data)
# mapes, forecast_t = forecast_ARIMAX_fixed_order(data)
# write_csv_file(csv_ARIMAX_orders, orders, data_dict=True)


# mapes_list = list(mapes.values())
# cleanmapes = [x for x in mapes_list if (x != np.inf)]
# plt.plot(list(mapes.values()))
# plt.show()
# print("fin; Average MAPE per meter: %f" % (sum(cleanmapes) / len(cleanmapes)))
# print("fin")

"""
TODO:
reate evaluations - accuracy (mape, etc) and time measurements
remove inf forcast mapes - meter 1060?
meter_selection/import_csv combine & update? 
"""

from data_gather import *
from order_estimation import *
from forecast import *
from import_csv import *
from clusterer import *

import matplotlib.pylab as plt

# csv_ARIMAX_orders = open('..\\masters_test\\data\\orders_ARIMAX.csv', "r")

num_of_meters = 1
window_size = datetime.timedelta(weeks=12)
data = gather_cer_data(num_of_meters, window_size=window_size, exog_check=True, forecast_check=True)
# data_check(data)

'''Cluster'''
# mode_orders = get_mode_orders(csv_ARIMAX_orders, 10)
# centroids = calculate_centroids(data, 3, 10, write_check=True)
# orders_clusters = estimate_orders_centroids()
# clusters = assign_clusters(data, centroids, orders_clusters, write_check=True)
# mapes_c, forecast_t_c = forecast_clusters(data, clusters)

'''SARIMA'''
# orders, order_estimation_t = estimate_SARIMAmodelorder(data)
# mapes, forecast_t = forecast_SARIMA_fixed_order(data, pdq=(0, 1, 2))
# csv_SARIMA_orders = open('..\\masters_test\\data\\orders_SARIMA.csv', "w+")
# write_csv_file(csv_SARIMA_orders, orders, data_dict=True)

'''ARIMAX'''
# orders, order_estimation_t = estimate_ARIMAXmodelorder(data)
mapes, forecast_t = forecast_ARIMAX_fixed_order(data, pdq=(0, 1, 2))
# write_csv_file(csv_ARIMAX_orders, orders, data_dict=True)


# mapes_list = list(mapes.values())
# cleanmapes = [x for x in mapes_list if (x != np.inf)]
# plt.plot(list(mapes.values()))
# plt.show()
# print("fin; Average MAPE per meter: %f" % (sum(cleanmapes) / len(cleanmapes)))

print("fin")

"""
TODO:
reate evaluations - accuracy (mape, etc) and time measurements
remove inf forcast mapes - meter 1060?
meter_selection/import_csv combine & update? 
"""

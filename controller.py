from data_gather import *
from order_estimation import *
from forecast import *
from import_csv import *

import matplotlib.pylab as plt

csv_ARIMAX_orders = open('..\\masters_test\\data\\orders_ARIMAX.csv', "w+")
num_of_meters = 2

data = gather_cer_data(exog_check=True)
data_check(data)

# orders, order_estimation_t = estimate_SARIMAmodelorder(data)
# mapes, forecast_t = forecast_SARIMA_fixed_order(data)

orders, order_estimation_t = estimate_ARIMAXmodelorder(data)
write_csv_file(csv_ARIMAX_orders, orders, data_dict=True)


# print("fin; Average MAPE per meter: %f" % (sum(mapes.values())/len(mapes)))
# print("fin")


"""
TODO:
create accuracy measurement - mape, etc
or create evaluations - accuracy and time measurements
remove inf forcast mapes - meter 1060?
"""
from data_gather import *
from order_estimation import *
from forecast import *
from import_csv import *

import matplotlib.pylab as plt


num_of_meters = 1
current = datetime.datetime(2012, 12, 12, 0, 0, 0, )
window_size = datetime.timedelta(weeks=12)
lookback_window_start = current - window_size


data = gather_cer_data_inc_init(current, num_of_meters, window_size=window_size, exog_check=True, forecast_check=True)

print("fin")
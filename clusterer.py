import pandas as pd
import numpy as np
import datetime
import time as t
import matplotlib.pylab as plt
import statsmodels.api as sm
import random
import psycopg2
import itertools
import warnings
from data_gather import *

from math import sqrt
from statistics import mode
from collections import Counter
from meter_selection import get_meters
# from parameter_selection import get_orders_csv
# from Import_CSV import *


def euclid_dist(t1, t2):
    return sqrt(sum((t1-t2)**2))


def DTWDistance(s1, s2,w):
    DTW={}

    w = max(w, abs(len(s1)-len(s2)))

    for i in range(-1,len(s1)):
        for j in range(-1,len(s2)):
            DTW[(i, j)] = float('inf')
    DTW[(-1, -1)] = 0

    for i in range(len(s1)):
        for j in range(max(0, i-w), min(len(s2), i+w)):
            dist= (s1[i]-s2[j])**2
            DTW[(i, j)] = dist + min(DTW[(i-1, j)],DTW[(i, j-1)], DTW[(i-1, j-1)])

    return sqrt(DTW[len(s1)-1, len(s2)-1])


def LB_Keogh(s1, s2, r):
    LB_sum=0
    for ind, i in enumerate(s1):

        lower_bound=min(s2[(ind-r if ind-r>=0 else 0):(ind+r)])
        upper_bound=max(s2[(ind-r if ind-r>=0 else 0):(ind+r)])

        if i>upper_bound:
            LB_sum=LB_sum+(i-upper_bound)**2
        elif i<lower_bound:
            LB_sum=LB_sum+(i-lower_bound)**2

    return sqrt(LB_sum)


def k_means_clust(data, num_clust, num_iter, w=5):
    centroids=random.sample(data,num_clust)
    counter=0
    for n in range(num_iter):
        counter+=1
        print(counter)
        assignments={}
        #assign data points to clusters
        for ind,i in enumerate(data):
            min_dist=float('inf')
            closest_clust=None
            for c_ind,j in enumerate(centroids):
                if LB_Keogh(i,j,5)<min_dist:
                    cur_dist=DTWDistance(i,j,w)
                    if cur_dist<min_dist:
                        min_dist=cur_dist
                        closest_clust=c_ind
            if closest_clust in assignments:
                assignments[closest_clust].append(ind)
            else:
                assignments[closest_clust]=[]

        # #recalculate centroids of clusters
        # for key in assignments:
        #     clust_sum=0
        #     for k in assignments[key]:
        #         clust_sum=clust_sum+float(data[k])
        #     centroids[key]=[m/len(assignments[key]) for m in clust_sum]

        #recalculate centroids of clusters
        for key in assignments:
            clust_sum=np.zeros(len(data[0]))
        for k in assignments[key]:
            clust_sum=np.add(clust_sum,data[k])
        centroids[key]=[m/len(assignments[key]) for m in clust_sum]

    return centroids


# endogs = []
# ids = []
# clusters = []
# best_fit_params = []
# times = []
# data_out = []
# number_of_meters = 4621
# # centroid_file_out = open('C:\\Users\\h93vw\\PycharmProjects\\ARIMATest\\data\\cer\\cluster_centroids%d.csv' % number_of_meters, 'w+')
# cluster_file_out = open('C:\\Users\\h93vw\\PycharmProjects\\ARIMATest\\data\\cer\\clusters.csv', 'w+')
# file_in = open('C:\\Users\\h93vw\\PycharmProjects\\ARIMATest\\data\\cer\\cluster_centroids.csv', 'r')
# data = gather_cer_data(number_of_meters)
# data_check(data)
#
# for item in data:
#     ids.append(item[0])
#     endogs.append([float(i) for i in item[1]])
#
#
# time_window = data[0][5]
# observed_window = data[0][6]
#
# # centroids = k_means_clust(endogs, 3, 10)
#
# centroids = read_csv_file(file_in)
#
#
#
# for centroid in centroids:
#     centroids[centroids.index(centroid)] = [float(i) for i in centroid]
# #     plt.plot(centroid)
#
# # # output to csv
# # write_csv_file(centroid_file_out, centroids)
#
# for item in endogs:
#     distance = []
#     for centroid in centroids:
#         distance.append(LB_Keogh(item, centroid, 5))
#     clusters.append(distance.index(min(distance)))
#
# for meter_id in ids:
#     data_out.append([meter_id, clusters[ids.index(meter_id)]])
#
# write_csv_file(cluster_file_out, data_out)

# for centroid in centroids:
#     individual_start_time = t.time()
#
#     endog = pd.Series(centroid, index=time_window, dtype='Float64')
#     aic = []
#     params = []
#
#     # Define the p, d and q parameters to take any value between 0 and 2
#     p = d = q = range(0, 3)
#     # seasonal order
#     sp = sq = sd = range(0, 2)
#
#     # Generate all different combinations of p, q and q triplets
#     pdq = list(itertools.product(p, d, q))
#
#     # Generate all different combinations of seasonal p, q and q triplets
#     seasonal_pdq = [(x[0], x[1], x[2], 48) for x in list(itertools.product(sp, sd, sq))]
#
#     warnings.filterwarnings("ignore")  # specify to ignore warning messages
#     i = 1
#     for param_seasonal in seasonal_pdq:
#         for param in pdq:
#             try:
#                 mod = sm.tsa.statespace.SARIMAX(endog,
#                                                 # exog,
#                                                 order=param,
#                                                 seasonal_order=param_seasonal,
#                                                 enforce_stationarity=False,
#                                                 enforce_invertibility=False)
#
#                 results = mod.fit(disp=0)
#
#                 print('{} - ARIMA{} - AIC:{}'.format(i, param, results.aic))
#                 i += 1
#                 aic.append(results.aic)
#                 params.append([param, param_seasonal])
#             except:
#                 continue
#     index = aic.index(min(aic))
#     best_fit_params.append(params[index])
#     individual_end_time = t.time()
#     times.append(individual_end_time - individual_start_time)
#
# plt.show()


'''Mode Orders'''
# csv_input = open('..\\masters_test\\data\\orders_ARIMAX.csv', "r")
# orders = get_orders_csv(csv_input)
# print((Counter(orders).most_common(5)))
#
# print("fin")

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
from meter_selection import get_orders_csv
from import_csv import *
# from Import_CSV import *


def get_mode_orders(csv_input, num=1):
    orders = get_orders_csv(csv_input)
    mode_orders = Counter(orders).most_common(num)
    print(mode_orders)
    return mode_orders

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


def calculate_centroids(data, num_clust=3, num_iter=10, write_check=False):

    endogs = []
    meter_ids = []

    number_of_meters = len(data)

    for row in data:
        meter_ids.append(row[0])
        endogs.append([float(i) for i in row[1]])

    centroids = k_means_clust(endogs, num_clust, num_iter)

    if write_check:
        cluster_file_out = open('..\\masters_test\\data\\clusters_centroids%d.csv' % number_of_meters, 'w+')
        write_csv_file(cluster_file_out, centroids)

    return centroids


def estimate_orders_centroids(centroids=[], csv_check=True, seasonal_check=False, write_check=False):
    orders = {}

    if csv_check:
        cluster_file_in = open('..\\masters_test\\data\\clusters_centroids.csv', 'r')
        centroids = read_csv_file(cluster_file_in)

    for centroid in centroids:
        centroids[centroids.index(centroid)] = [float(i) for i in centroid]
    #     plt.plot(centroid)
    # plt.show()

    for centroid in centroids:
        # individual_start_time = t.time()

        # endog = pd.Series(centroid, index=time_window, dtype='Float64')
        endog = centroid
        aic = []
        params = []

        # Define the p, d and q parameters to take any value between 0 and 2
        p = d = q = range(0, 3)
        # seasonal order
        sp = sq = sd = range(0, 2)

        # Generate all different combinations of p, q and q triplets
        pdq = list(itertools.product(p, d, q))

        if seasonal_check:
            # Generate all different combinations of seasonal p, q and q triplets
            seasonal_pdq = [(x[0], x[1], x[2], 48) for x in list(itertools.product(sp, sd, sq))]
        else:
            seasonal_pdq = [(0, 0, 0, 48)]

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

                    print('{} - ARIMA{} - AIC:{}'.format(i, param, results.aic))
                    i += 1
                    aic.append(results.aic)
                    params.append([param, param_seasonal])
                except:
                    continue
        index = aic.index(min(aic))
        orders[centroids.index(centroid)] = params[index]
        # individual_end_time = t.time()
        # times[centroids.index(centroid)] = (individual_end_time - individual_start_time)
    for key, value in orders.items():
        print("{} - {}".format(key, value))
    if write_check:
        cluster_file_out = open('..\\masters_test\\data\\clusters_orders.csv', 'w+')
        write_csv_file(cluster_file_out, orders, data_dict=True)

    return orders


def assign_clusters(data, centroids=[], cluster_orders={}, csv_check=True, write_check=False):

    endogs = {}
    clusters = {}

    number_of_meters = len(data)

    for row in data:
        endogs[row[0]] = ([float(i) for i in row[1]])

    if csv_check:
        cluster_file_in = open('..\\masters_test\\data\\clusters_centroids.csv', 'r')
        centroids = read_csv_file(cluster_file_in)

    for centroid in centroids:
        centroids[centroids.index(centroid)] = [float(i) for i in centroid]
    #     plt.plot(centroid)
    # plt.show()

    for key, value in endogs.items():
        distances = []
        for centroid in centroids:
            distances.append(LB_Keogh(value, centroid, 5))
        clusters[key] = cluster_orders[distances.index(min(distances))]

    if write_check:
        cluster_file_out = open('..\\masters_test\\data\\clusters_assignment%d.csv' % number_of_meters, 'w+')
        write_csv_file(cluster_file_out, clusters, data_dict=True)

    return clusters


# print("fin")

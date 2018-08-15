from import_csv import *
from ast import literal_eval

def get_meters(number_of_meters=4621, start=1000):
    csv_input = open('..\\masters_test\\data\\meterreadingsall.csv', "r")
    data = read_csv_file_header(csv_input, ',')
    meters = sorted(int(i[0]) for i in data if int(i[1]) >= 25726)
    start_index = meters.index(start)
    meters = meters[0+start_index:number_of_meters+start_index]
    return meters


def get_orders_csv(csv_input):
    data = read_csv_file(csv_input, ',')
    orders = [literal_eval(row[1]) for row in data]
    # times = [float(i) for i in data[2]]
    # avg_time = (sum(times)/len(times))
    # print(avg_time)
    return orders

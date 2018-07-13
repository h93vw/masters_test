from import_csv import read_csv_file_header


def get_meters(number_of_meters=4621, start=1000):
    csv_input = open('..\\masters_test\\data\\meterreadingsall.csv', "r")
    data = read_csv_file_header(csv_input, ',')
    meters = sorted(int(i[0]) for i in data if int(i[1]) >= 25726)
    start_index = meters.index(start)
    meters = meters[0+start_index:number_of_meters+start_index]
    return meters

import csv


def read_csv_file(in_file, delimiter=','):
    # infile = open('C:\\Users\\h93vw\\IdeaProjects\\loadForcast\\data\\data.csv', "r")
    reader = csv.reader(in_file, delimiter=delimiter)
    data = []
    for row in reader:
        data.append(row)
    in_file.close()
    return data


def read_csv_file_header(in_file, delimiter=',', num_header_lines=1):
    # infile = open('C:\\Users\\h93vw\\IdeaProjects\\loadForcast\\data\\data.csv', "r")
    reader = csv.reader(in_file, delimiter=delimiter)
    for i in range(num_header_lines):
        next(reader, None)  # skip the headers
    data = []
    for row in reader:
        data.append(row)
    in_file.close()
    return data


def write_csv_file(file_out, data_out, line_terminator='\n', data_dict=False):
    # file_out = open('C:\\Users\\Jay\\Desktop\\data\\CER\\Modified\\File%d.txt' % file, 'w+')

    writer = csv.writer(file_out, lineterminator=line_terminator)
    if not data_dict:
        for row in data_out:
            writer.writerow(row)
    else:
        for row in data_out.items():
            writer.writerow(row)
    file_out.close()

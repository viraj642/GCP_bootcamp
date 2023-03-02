import csv
import sys
import os
from faker import Faker
from random import randint, choice

# Define No. of rows and Columns for csv file
if len(sys.argv)==4:
	FILE_PATH=sys.argv[1]
	ROWS=int(sys.argv[3])
	COLUMNS=int(sys.argv[2])
else:
    FILE_PATH="data.csv"
    ROWS=10000
    COLUMNS=10


def generate_dummy_data(num_rows, num_columns):
    fake = Faker()
    data = []
    data_type = ['number', 'text', 'boolean', 'date', 'timestamp']
    for i in range(num_rows):
        row = []
        for j in range(num_columns):
            
            if data_type[j%5] == 'number':
                row.append(randint(0, 100))
            elif data_type[j%5] == 'text':
                row.append(fake.text())
            elif data_type[j%5] == 'boolean':
                row.append(choice([True, False]))
            elif data_type[j%5] == 'date':
                row.append(fake.date())
            elif data_type[j%5] == 'timestamp':
                row.append(fake.date_time())
        data.append(row)
    return data

def append_to_csv(filename, data):
    with open(filename, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)

def create_or_append_csv(filename, num_rows, num_columns):
    if not os.path.isfile(filename):
        # file doesn't exist, create a new file and add header row
        header = ['Column ' + str(i+1) for i in range(num_columns)]
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)

    # generate dummy data and append to file
    data = generate_dummy_data(num_rows, num_columns)
    append_to_csv(filename, data)


create_or_append_csv(FILE_PATH, ROWS, COLUMNS)



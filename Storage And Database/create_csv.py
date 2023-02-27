import csv
import random
import sys
import string
from datetime import datetime, timedelta

# Define No. of rows and Columns for csv file
if len(sys.argv)==4:
	FILE_PATH=sys.argv[1]
	ROWS=int(sys.argv[2])
	COLUMNS=int(sys.argv[3])
else:
    FILE_PATH="data.csv"
    ROWS=10000
    COLUMNS=10


# Define column names and data types
col_names = ['col_' + str(i) for i in range(COLUMNS)]
col_types = ['int', 'float', 'str', 'bool', 'date', 'datetime']


# Create a list of rows and append all the rows to the list.
rows = []
for i in range(ROWS):
    row = []
    for j in range(COLUMNS):
        if col_types[j % len(col_types)] == 'int':
            row.append(random.randint(0, 100))
        elif col_types[j % len(col_types)] == 'float':
            row.append(random.uniform(0, 1))
        elif col_types[j % len(col_types)] == 'str':
            row.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=10)))
        elif col_types[j % len(col_types)] == 'bool':
            row.append(random.choice([True, False]))
        elif col_types[j % len(col_types)] == 'date':
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2022, 12, 31)
            row.append((start_date + timedelta(days=random.randint(0, (end_date - start_date).days))).strftime('%Y-%m-%d'))
        elif col_types[j % len(col_types)] == 'datetime':
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2022, 12, 31)
            row.append((start_date + timedelta(days=random.randint(0, (end_date - start_date).days))).strftime('%Y-%m-%d %H:%M:%S'))

    rows.append(row)

# Create a csv file and creating writer object to write rows to csv file.
with open(FILE_PATH, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(col_names)
    writer.writerows(rows)


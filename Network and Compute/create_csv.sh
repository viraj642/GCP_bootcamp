#!/bin/bash

# set the rows and columns
rows=$1
columns=$2

# check for file exists or not
if [ -f "data.csv" ]; 
then

# if file exist the it will be printed 
    echo "File alredy exist"
else
# Create a file named "data.csv"
    touch data.csv
fi


# Generate the data and append to the file
for i in $(seq 1 $rows); do
  line=""
  for j in $(seq 1 $columns); do
    line+=","
  done
  echo ${line:1} >> data.csv
done
"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import csv
import json
import os


from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
import pandas as pd
from confluent_kafka.serialization import StringSerializer


employee_topic_name = "bf_employee_salary"
csv_file = 'Employee_Salaries.csv'

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
     

class DataHandler:
    '''
    Your data handling logic goes here. 
    You can also implement the same logic elsewhere. Your call
    '''
    def __init__(self):
        pass

    def read_csv(self, csv_file):
        # Use pandas for efficient CSV parsing and handling
        df = pd.read_csv(csv_file)
        return df
        
    def transform(self, df):
        # Filter and transform data based on business requirements
        res = []
        depts = set(['ECC','CIT','EMS'])  # Only process these 3 departments
        for index, row in df.iterrows():
            dept = row['Department']
            try:
                salary = int(row['Salary'])
                # Extract year from date format: MM-DD-YYYY
                hire_year = int(row['Initial Hire Date'].split('-')[2])
            except:
                # Skip records with null/invalid data to maintain data quality
                print(f'null found at {index}')
                continue
            # Filter: only employees hired in 2010 or later from specified departments
            if dept in depts and hire_year >= 2010:
                res.append([dept, salary])
                
        return res

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = DataHandler()
    producer = salaryProducer()
    
    # Read and transform CSV data
    df = reader.read_csv(csv_file)
    lines = reader.transform(df)
    print(f"Total entries to produce: {len(lines)}")
    
    # Produce messages to Kafka topic
    for line in lines:
        emp = Employee.from_csv_line(line)
        # Use department as key for partitioning - ensures same dept goes to same partition
        # This enables parallel processing by consumer groups and maintains order per department
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        # Poll to handle delivery callbacks and keep connection alive
        producer.poll(1)
    
    # Flush ensures all messages are sent before exiting
    producer.flush()
    print(f"Successfully produced {len(lines)} messages to topic '{employee_topic_name}'")
    